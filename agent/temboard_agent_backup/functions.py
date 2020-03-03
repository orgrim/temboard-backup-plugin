# -*- coding: utf-8 -*-

import logging
import os.path
from temboardagent.toolkit import taskmanager
from temboardagent.errors import HTTPError
from pickle import dumps as pickle


logger = logging.getLogger('backup')


def check_tool(raw):
    # define a validator for the flavour that imports to proper module which
    # implements the API for the tool. If no module is found in the current
    # directory then fails

    if not raw:
        raise ValueError("backup tool must be set")

    if raw not in ['pitrery', 'pgbackrest']:
        raise ValueError("unsupported backup tool: {}".format(raw))

    return raw


def list_backup_tasks(socket):
    task_list = []
    tasks = taskmanager.TaskManager.send_message(
        socket,
        taskmanager.Message(taskmanager.MSG_TYPE_TASK_LIST, ''),
        authkey=None,
    )

    for task in tasks:
        if task['worker_name'] in ('backup_worker', 'purge_worker'):
            task_list.append(task)

    return task_list


def schedule_operation(what, when, config, expire=86400):
    options = {'config': pickle(config)}
    try:
        res = taskmanager.schedule_task(
            what + '_worker',
            options=options,
            start=when,
            listener_addr=str(os.path.join(config.temboard.home,
                                           '.tm.socket')),
            expire=expire,
        )
    except Exception as e:
        logger.exception(str(e))
        raise HTTPError(500, "Unable to schedule {}".format(what))

    if res.type == taskmanager.MSG_TYPE_ERROR:
        logger.error(res.content)
        raise HTTPError(500, "Unable to schedule {}".format(what))

    return res.content
