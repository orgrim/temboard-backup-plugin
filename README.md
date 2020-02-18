# temBoard backup plugin

This plugin allow temBoard to manage PITR backups created with pitrery
or pgBackRest.

It is divided into two part, one for the agent installed on the
PostgreSQL servers, one for Web UI.

The agent plugin provides a unified REST API to interact with pitrery
or pgBackRest. The underlying tool must be setup prior to using the
plugin.

Each plugin part has its README.md which documents its installation,
configuration and operation.

This plugin is licensed under the PostgreSQL License.

Development is done through Github:

https://github.com/orgrim/temboard-backup-plugin

Any contributions are welcome!
