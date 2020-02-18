# -*- coding: utf-8 -*-

import logging

from temboardagent.routing import RouteSet
from temboardagent.toolkit.configuration import OptionSpec
from temboardagent.toolkit.validators import file_
from temboardagent.types import T_OBJECTNAME
import datetime
import os.path
from pickle import dumps as pickle, loads as unpickle
from temboardagent.errors import HTTPError
from temboardagent.toolkit import taskmanager

from . import functions

import pgbackrest
import pitrery


logger = logging.getLogger('backup')
routes = RouteSet(prefix=b'/backup')

__version__ = '1.0'


class _BackupTool(object):
    def __new__(self, tool):
        if tool == 'pitrery':
            return pitrery
        return pgbackrest


@routes.get(b'', check_session=False)
def get_plugin_info(http_context, app):
    return {
        'plugin': 'backup',
        'version': __version__,
        'configuration': {
            'tool': app.config.backup.tool,
            'configfile': app.config.backup.configfile,
            'stanza': app.config.backup.stanza,
            'path': app.config.backup.path
            }
        }


@routes.get(b'/tool')
def get_tool_info(http_context, app):
    return _BackupTool(app.config.backup.tool).info(app)


@routes.get(b'/config')
def get_backups_config(http_context, app):
    return _BackupTool(app.config.backup.tool).config(app)


@routes.get(b'/status')
def get_backups_progress(http_context, app):
    task_list = []
    tasks = taskmanager.TaskManager.send_message(
        str(os.path.join(app.config.temboard.home, '.tm.socket')),
        taskmanager.Message(taskmanager.MSG_TYPE_TASK_LIST, ''),
        authkey=None,
    )

    for task in tasks:
        if task['worker_name'] != 'backup_worker':
            continue
        # Convert datetimes to string to be able to JSONify them in the
        # response
        dt = task['start_datetime'].strftime("%Y-%m-%dT%H:%M:%SZ")
        task['start_datetime'] = dt
        dt = task['stop_datetime'].strftime("%Y-%m-%dT%H:%M:%SZ")
        task['stop_datetime'] = dt

        task_list.append(task)
        logger.debug(task)
    return task_list


# XXX allow to choose a task id to cancel
@routes.post(b'/cancel')
def post_cancel_backup(http_context, app):
    tasks = taskmanager.TaskManager.send_message(
        str(os.path.join(app.config.temboard.home, '.tm.socket')),
        taskmanager.Message(taskmanager.MSG_TYPE_TASK_CANCEL, dict(task_id='test')),
        authkey=None,
    )
    return tasks


@routes.get(b'/list')
def get_backups_list(http_context, app):
    return _BackupTool(app.config.backup.tool).list(app)


@routes.post(b'/purge')
def post_run_purge(http_context, app):
    return _BackupTool(app.config.backup.tool).purge(app)


@routes.post(b'/create')
def post_run_backup(http_context, app):
    dt = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)
    logger.debug(dt)

    try:
        res = taskmanager.schedule_task(
            'backup_worker',
            id='test',
            options={'config': pickle(app.config)},
            start=dt,
            listener_addr=str(os.path.join(app.config.temboard.home,
                                           '.tm.socket')),
            expire=0,
        )
    except Exception as e:
        logger.exception(str(e))
        raise HTTPError(500, "Unable to schedule backup")

    if res.type == taskmanager.MSG_TYPE_ERROR:
        logger.error(res.content)
        raise HTTPError(500, "Unable to schedule backup")

    return res.content


@taskmanager.worker(pool_size=10)
def backup_worker(config):
    config = unpickle(config)
    return _BackupTool(config.backup.tool).backup(config)


# Use POST /restore to restore for real
@routes.post(b'/restore')
def post_run_restore(http_context, app):
    dt = datetime.datetime.utcnow()

    try:
        res = taskmanager.schedule_task(
            'restore_worker',
            id='test',
            options={'config': pickle(app.config)},
            start=dt,
            listener_addr=str(os.path.join(app.config.temboard.home,
                                           '.tm.socket')),
            expire=0,
        )
    except Exception as e:
        logger.exception(str(e))
        raise HTTPError(500, "Unable to schedule restore")

    if res.type == taskmanager.MSG_TYPE_ERROR:
        logger.error(res.content)
        raise HTTPError(500, "Unable to schedule restore")

    return res.content


@taskmanager.worker(pool_size=10)
def restore_worker(config):
    config = unpickle(config)
    return _BackupTool(config.backup.tool).restore(config)


# Use GET /restore for a dry run
@routes.get(b'/restore')
def get_restore_dry_run(http_context, app):
    return _BackupTool(app.config.backup.tool).restore_dry_run(app)


@routes.get(b'/list/%s' % (T_OBJECTNAME))
def get_backup_info(http_context, app):
    pass


class BackupPlugin(object):
    s = 'backup'
    option_specs = [
        OptionSpec(s, 'tool', default='pgbackrest',
                   validator=functions.check_tool),
        OptionSpec(s, 'path', default=None, validator=file_),
        OptionSpec(s, 'configfile', default=None, validator=file_),
        OptionSpec(s, 'stanza', default=None)
    ]

    def __init__(self, app, **kw):
        self.app = app
        self.app.config.add_specs(self.option_specs)

        # Use the same logging level as the whole agent
        logger.setLevel(self.app.config.logging.level)

    def load(self):
        if self.app.config.backup.tool == 'pitrery':
            if self.app.config.backup.path is None:
                self.app.config.backup.path = 'pitrery'
        else:
            if self.app.config.backup.path is None:
                self.app.config.backup.path = 'pgbackrest'

        self.app.router.add(routes)
        for route in routes:
            logger.debug(route)

    def unload(self):
        self.app.router.remove(routes)
        self.app.config.remove_specs(self.option_specs)
