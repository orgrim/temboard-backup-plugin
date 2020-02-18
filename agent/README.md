# Backup Agent

## Setup / Install for developpement

- Setup project with `python setup.py egg_info` in this directory.
- Activate a virtualenv 
- Install the project with `pip install -e .`.

## Configuration

A `backup` section can be added to temboard-agent.conf

* tool = { pitrery | pgbackrest }
* configfile = path to the configuration file of the tool

## API

Everything shall be under `/backup`.

Routes:
* '' (eg /backup only). GET. report information about the plugin and its configuration 
* '/config'. GET. report details about the configuration of the backup
* '/status'. GET. tell if a backup is in progress
* '/list'. GET. give the list of backups
* '/purge'. POST. schedule a general purge confirming the retention policy. parameters ?
* '/create'. POST. schedule a backup. param: datetime
* '/restore'. POST. schedule a restore. param: target_time? xid?
* '/%id'. GET. give info about a backup
* '/%id'. DELETE. remove a backup if possible
* '/%id/restore'. POST. restore a backup -> recovery_target = immediate 

