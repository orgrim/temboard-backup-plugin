import json
import logging
import re
from datetime import datetime
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
    if rc != 0:
        return []

    info = json.loads(out)

    normalized = []
    for b in info['backups']:

        # Ensure the reported space used for the backup is in bytes. The value
        # comes from du -sh, so there may be a size suffix to process
        if re.match(r'\d$', b['space_used']) is not None:
            stored_size = int(b['space_used'])
        else:
            stored_size = int(b['space_used'][0:-1])
            suffix = b['space_used'][-1]

            candidates = ['K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
            if suffix in candidates:
                for s in candidates:
                    stored_size = stored_size * 1024
                    if suffix == s:
                        break
            else:
                logger.error(
                    "Could not compute size of the stored backup: "
                    "invalid value {}".format(b['space_used'])
                )
                stored_size = None

        tbs = []
        db_size = 0
        for t in b['tablespaces']:
            # pg_size_unpretty the size of the tablespace from bytes, kB, MB,
            # GB or TB
            (tbs_size, unit) = t['size'].split()
            for u in ['bytes', 'kB', 'MB', 'GB', 'TB']:
                if unit == u:
                    break
                tbs_size = int(tbs_size) * 1024

            db_size = db_size + tbs_size
            del t['size']

            # Exclude pg_global and pg_default from the output
            if t['location'] is not None:
                tbs.append(t)

        # Convert the stop time of the backup to a timestamp (seconds from
        # Epoch)
        stop_time = datetime.strptime(b['stop_time'], "%Y-%m-%d %H:%M:%S %Z")
        timestamp = int((stop_time - datetime(1970,1,1)).total_seconds())

        # pitrery only creates full backups so the sizes between db and backup
        # are the same
        normalized.append({
            'db_size': db_size,
            'backup_size': db_size,
            'stored_db_size': stored_size,
            'stored_backup_size': stored_size,
            'type': 'full',
            'stop_time': timestamp,
            'set': b['directory'],
            'reference': None,
            'tablespaces': tbs
        })

    return normalized


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
