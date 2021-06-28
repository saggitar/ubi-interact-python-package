def diff_dicts(compare, expected, **kwargs):
    import json
    left = json.dumps(compare, indent=2, sort_keys=True)
    right = json.dumps(expected, indent=2, sort_keys=True)

    import difflib
    diff = difflib.unified_diff(left.splitlines(True), right.splitlines(True), **kwargs)
    return list(diff)
