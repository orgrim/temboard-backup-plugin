from temboardagent.command import (
    exec_command,
    )


def base_cmd(app):
    conf = app.config.backup
    c = [conf.path]
    if conf.configfile is not None:
        c += ['--config', conf.configfile]
    return c


def info(app):
    (rc, out, err) = exec_command(
        [app.config.backup.path, "version"]
        )
    v = out.strip()
    return {"version_string": v}


def list(app):
    pass
