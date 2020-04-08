# -*- coding: utf-8 -*-

import logging
import json
import re
from temboardagent.errors import UserError
from temboardagent.command import (
    exec_command,
    )


logger = logging.getLogger('backup')


def base_cmd(config):
    c = [config.backup.path]
    if config.backup.configfile is not None:
        c += ['--config', config.backup.configfile]
    if config.backup.stanza is not None:
        c += ['--stanza', config.backup.stanza]
    return c


def info(app):
    cmd = base_cmd(app.config) + ['version']
    logger.debug(' '.join(cmd))
    (rc, out, err) = exec_command(cmd)
    if rc != 0:
        raise UserError("Could not get version of pgBackRest")
    v = out.strip().split()[1]
    return {
        "tool": "pgBackRest",
        "version": v,
        "supported": True
    }


def config(app):
    result = []
    with open(app.config.backup.configfile) as c:
        for rawline in c:
            line = rawline.strip()
            if len(line) > 0:
                result.append(line.strip())
    return result


def list(app):
    cmd = base_cmd(app.config) + ['--output', 'json', 'info']
    logger.debug(' '.join(cmd))
    (rc, out, err) = exec_command(cmd)
    if rc != 0:
        return []

    # The output is a list of stanzas, we have specified the stanza so there
    # shall be only one element.
    info = json.loads(out)[0]

    normalized = []
    tbspat = re.compile(r'(\S+) \((\d+)\) => (.+)$')
    for b in info['backup']:

        backup = {
            'db_size': b['info']['size'],
            'backup_size': b['info']['delta'],
            'stored_db_size': b['info']['repository']['size'],
            'stored_backup_size': b['info']['repository']['delta'],
            'type': b['type'],
            'stop_time': b['timestamp']['stop'],
            'set': b['label'],
            'reference': b['reference']
        }

        # To get the tablespaces details
        cmd = base_cmd(app.config) + ['--set', b['label'], 'info']
        (rc, out, err) = exec_command(cmd)
        if rc != 0:
            logger.error("Could not get set info for {}".format(b['label']))
            logger.error('\n'.join(err))
            backup['tablespaces'] = None
        else:
            backup['tablespaces'] = []
            start = 0
            for l in out.splitlines():
                if start:
                    m = tbspat.match(l.strip())
                    if m is not None:
                        backup['tablespaces'].append({
                            'name': m.group(1),
                            'oid': m.group(2),
                            'location': m.group(3)
                        })
                if l.strip() == "tablespaces:":
                    start = 1

        normalized.append(backup)
    return normalized


def purge(config):
    cmd = base_cmd(config) + ['expire']
    logger.debug(' '.join(cmd))

    (rc, out, err) = exec_command(cmd)
    result = {'rc': rc,
              'stdout': out.split('\n'),
              'stderr': err.split('\n')}
    logger.debug(out)
    logger.debug(err)
    if rc != 0:
        raise UserError(json.dumps(result))
    return json.dumps(result)


def backup(config):
    cmd = base_cmd(config) + ['backup']
    logger.debug(' '.join(cmd))

    (rc, out, err) = exec_command(cmd)
    result = {'rc': rc,
              'stdout': out.split('\n'),
              'stderr': err.split('\n')}
    logger.debug(out)
    logger.debug(err)
    if rc != 0:
        raise UserError(json.dumps(result))
    return json.dumps(result)
