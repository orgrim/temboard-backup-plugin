def check_tool(raw):
    # define a validator for the flavour that imports to proper module which
    # implements the API for the tool. If no module is found in the current
    # directory then fails

    if not raw:
        raise ValueError("backup tool must be set")

    if raw not in ['pitrery', 'pgbackrest']:
        raise ValueError("unsupported backup tool: {}".format(raw))

    return raw
