def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f'{name}{a}_')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, f'{name}{i}_')
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def compare_json(old, new):
    """
    Compares two flat JSON dicts and returns a diff of changed keys.
    """
    diff = {}
    for key in new:
        if key not in old or old[key] != new[key]:
            diff[key] = {
                "old": old.get(key),
                "new": new[key]
            }
    return diff


def compute_diff(old: dict, new: dict) -> dict:
    diff = {}
    keys = set(old.keys()).union(new.keys())

    for key in keys:
        old_val = old.get(key)
        new_val = new.get(key)
        if old_val != new_val:
            diff[key] = {"old": old_val, "new": new_val}

    return diff
