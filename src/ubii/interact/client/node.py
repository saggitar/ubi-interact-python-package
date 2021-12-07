import logging
import re
from functools import cached_property, partial

from .topic import TopicClient
from ..types import IClient


class Client(IClient):
    @cached_property
    def log(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @cached_property
    def topic_client(self):
        return TopicClient(self)

    @cached_property
    def hub(self):
        from ubii.interact.hub import Ubii
        return Ubii.instance

    def __str__(self):
        strip = partial(re.sub, r'\s', '')

        def repl(match: re.Match):
            return f"{match.group(1)}:{match.group(2)}"

        fmt_arg = partial(re.sub, r'"(\w+)":(.+)', repl)

        values = {
            'cls': self.__class__.__name__,
            'content': ', '.join(fmt_arg(strip(p)) for p in type(self).to_json(self)[1:-1].split(',')),
        }
        fmt = ', '.join('{' + k + '}' for k, v in values.items() if v)
        return f"<{fmt.format(**values)}>"
