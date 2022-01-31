import typing as t
from operator import add


def merge_dicts(base: t.Dict, merge: t.Dict, op: t.Callable = add) -> t.Dict:
    def merge_op(left, right):
        if isinstance(left, dict) and isinstance(right, dict):
            return merge_dicts(left, right, op=op)
        else:
            return op(left, right)

    return {**base, **merge, **{k: merge_op(base[k], merge[k]) for k in base if k in merge}}
