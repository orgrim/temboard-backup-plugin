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
* '/status'. GET. tell if a backup or a purge is in progress
* '/cancel/%taskid'. POST. cancel a backup previously scheduled by /create or a purge previously scheduled by /purge
* '/list'. GET. give the list of backups
* '/purge'. POST. schedule a general purge confirming the retention policy. parameters ?
* '/create'. POST. schedule a backup. param: datetime

