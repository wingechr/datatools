import re


def parse_sql_type(type_str):
    name, args = re.match(r"^([^(]+)(|\(.*\))$", type_str).groups()
    name = name.strip().upper()
    args = re.sub("[()]", "", args).strip()
    if args:
        args = tuple(int(a.strip()) for a in args.split(","))
    else:
        args = ()
    return name, args
