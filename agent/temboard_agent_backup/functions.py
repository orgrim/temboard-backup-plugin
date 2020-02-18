# -*- coding: utf-8 -*-

from temboardagent.toolkit import taskmanager


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
        if task['worker_name'] == 'backup_worker':
            task_list.append(task)

    return task_list
