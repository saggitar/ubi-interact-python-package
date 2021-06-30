import contextlib
import functools
from typing import Tuple, Any, Callable, Coroutine
import asyncio
from asyncio import gather, Task
from functools import wraps
import logging


async def gather_dict(tasks: dict):
    async def mark(key, coro):
        return key, await coro

    return {
        key: result
        for key, result in await gather(
            *(mark(key, coro) for key, coro in tasks.items())
        )
    }


def _wrapper(fun, logger: logging.Logger = None,  message='', message_args: Tuple[Any, ...] = ()):
    def _handle_task_result(task: asyncio.Task, *, logger: logging.Logger, message: str, message_args: Tuple[Any, ...] = ()):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(message, *message_args)

    logger = logger or logging.getLogger()

    @wraps(fun)
    def _with_logging(*args, **kwargs):
        task: asyncio.Task = fun(*args, **kwargs)
        task.add_done_callback(functools.partial(_handle_task_result, logger=logger, message=message, message_args=message_args))
        return task

    return _with_logging


@contextlib.contextmanager
def log_exceptions_in_background_tasks(logger: logging.Logger = None, message='', *message_args):
    original = asyncio.create_task
    asyncio.create_task = _wrapper(asyncio.create_task, logger=logger, message=message, message_args=message_args)
    yield
    asyncio.create_task = original


def once(func: Callable[..., Coroutine]) -> Callable[..., Task]:
    future = None

    @wraps(func)
    def once_wrapper(*args, **kwargs):
        nonlocal future
        if not future:
            future = asyncio.create_task(func(*args, **kwargs))
        return future

    return once_wrapper
