# -*- coding: utf-8 -*-

import logging
import json
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

T_OPERATION_ID = b'(^[0-9a-f]{8}$)'


class _BackupTool(object):
    def __new__(self, tool):
        if tool == 'pitrery':
            return pitrery
        return pgbackrest


@routes.get(b'', check_session=False)
def get_plugin_info(http_context, app):
    tool_info = _BackupTool(app.config.backup.tool).info(app)
    info = {
        'plugin': 'backup',
        'version': __version__,
        'configuration': {
            'configfile': app.config.backup.configfile,
            'stanza': app.config.backup.stanza,
            'path': app.config.backup.path
            }
        }
    info['configuration'].update(tool_info)
    return info


@routes.get(b'/config')
def get_backups_config(http_context, app):
    return _BackupTool(app.config.backup.tool).config(app)


@routes.get(b'/status')
def get_backups_progress(http_context, app):
    task_list = []

    tasks = functions.list_backup_tasks(
        str(os.path.join(app.config.temboard.home, '.tm.socket'))
    )

    for task in tasks:
        res = {}
        # Convert datetimes to string to be able to JSONify them in the
        # response
        res['start_datetime'] = task['start_datetime'].strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        if task['stop_datetime'] is not None:
            res['stop_datetime'] = task['stop_datetime'].strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        else:
            res['stop_datetime'] = None

        # Make the output look better
        if task['output'] is not None:
            output = json.loads(task['output'])
            res.update(output)

        # Make the status code human readable, see taskmanager.py
        if task['status'] == taskmanager.TASK_STATUS_DEFAULT:
            res['status'] = "TASK_STATUS_DEFAULT"
        elif task['status'] == taskmanager.TASK_STATUS_SCHEDULED:
            res['status'] = "TASK_STATUS_SCHEDULED"
        elif task['status'] == taskmanager.TASK_STATUS_QUEUED:
            res['status'] = "TASK_STATUS_QUEUED"
        elif task['status'] == taskmanager.TASK_STATUS_DOING:
            res['status'] = "TASK_STATUS_DOING"
        elif task['status'] == taskmanager.TASK_STATUS_DONE:
            res['status'] = "TASK_STATUS_DONE"
        elif task['status'] == taskmanager.TASK_STATUS_FAILED:
            res['status'] = "TASK_STATUS_FAILED"
        elif task['status'] == taskmanager.TASK_STATUS_CANCELED:
            res['status'] = "TASK_STATUS_CANCELED"
        elif task['status'] == taskmanager.TASK_STATUS_ABORTED:
            res['status'] = "TASK_STATUS_ABORTED"
        elif task['status'] == taskmanager.TASK_STATUS_ABORT:
            res['status'] = "TASK_STATUS_ABORT"
        else:
            res['status'] = task['status']

        for i in ['id', 'expire', 'redo_interval']:
            res[i] = task[i]
        task_list.append(res)
    return task_list


@routes.post(b'/cancel/' + T_OPERATION_ID)
def post_cancel_backup(http_context, app):
    task_id = http_context['urlvars'][0]

    tasks = functions.list_backup_tasks(
        str(os.path.join(app.config.temboard.home, '.tm.socket'))
    )

    task = None
    for t in tasks:
        if task_id == t['id']:
            task = t

    if task is None:
        raise HTTPError(404, "Operation not found in current task list")

    # Cancel or abort the task depending on its status
    if task['status'] < taskmanager.TASK_STATUS_DOING:
        msg = taskmanager.MSG_TYPE_TASK_CANCEL
        response = "cancel signal sent"
    elif task['status'] == taskmanager.TASK_STATUS_DOING:
        msg = taskmanager.MSG_TYPE_TASK_ABORT
        response = "abort signal sent"
    else:
        # Send a 410 Gone when the task is done or already cancelled or aborted
        raise HTTPError(410, "Operation has already completed")

    taskmanager.TaskManager.send_message(
        str(os.path.join(app.config.temboard.home, '.tm.socket')),
        taskmanager.Message(msg, dict(task_id=task_id)),
        authkey=None,
    )

    return {'response': response}


@routes.get(b'/list')
def get_backups_list(http_context, app):
    return _BackupTool(app.config.backup.tool).list(app)


@routes.post(b'/purge')
def post_run_purge(http_context, app):
    return _BackupTool(app.config.backup.tool).purge(app)


@routes.post(b'/create')
def post_run_backup(http_context, app):
    dt = datetime.datetime.utcnow()
    logger.debug(dt)

    try:
        res = taskmanager.schedule_task(
            'backup_worker',
            options={'config': pickle(app.config)},
            start=dt,
            listener_addr=str(os.path.join(app.config.temboard.home,
                                           '.tm.socket')),
            expire=3600,
        )
    except Exception as e:
        logger.exception(str(e))
        raise HTTPError(500, "Unable to schedule backup")

    if res.type == taskmanager.MSG_TYPE_ERROR:
        logger.error(res.content)
        raise HTTPError(500, "Unable to schedule backup")

    return res.content


@taskmanager.worker(pool_size=1)
def backup_worker(config):
    config = unpickle(config)
    return _BackupTool(config.backup.tool).backup(config)




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
