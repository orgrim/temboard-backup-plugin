import json
import logging
import re
from temboardagent.errors import UserError
from temboardagent.command import (
    exec_command,
    )


logger = logging.getLogger('backup')


def base_cmd(config):
    conf = config.backup
    c = [conf.path]
    if conf.configfile is not None:
        c += ['-c', conf.configfile]
    return c


def get_version(app):
    (rc, out, err) = exec_command(
        [app.config.backup.path, "-V"]
        )
    if rc != 0:
        raise UserError

    m = re.match(r'pitrery ([\d.]+)', out.strip())
    if m is None:
        raise UserError

    return m.group(1)


def check_version(app):
    vs = get_version(app)

    if int(vs.replace('.', '0')) >= 202:
        return True
    else:
        return False


def info(app):
    return {
        "tool": "pitrery",
        "version": get_version(app),
        "supported": check_version(app)
    }


def list(app):
    if not check_version(app):
        raise UserError

    cmd = base_cmd(app.config) + ['list', '-j']
    logger.debug(' '.join(cmd))

    (rc, out, err) = exec_command(cmd)
    logger.debug(out)
    if rc != 0:
        return []
    else:
        return json.loads(out)


def config(app):
    result = []
    with open(app.config.backup.configfile) as c:
        for line in c:
            if re.match(r'^[^#]', line.strip()):
                result.append(line.strip())
    return result


def purge(config):
    cmd = base_cmd(config) + ['purge']
    logger.debug(' '.join(cmd))
    (rc, out, err) = exec_command(cmd)
    result = {
        'rc': rc,
        'stdout': out.split('\n'),
        'stderr': err.split('\n')
    }
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
