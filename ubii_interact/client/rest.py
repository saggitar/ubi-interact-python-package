import logging
import os

from ..util import UBII_SERVICE_URL

log = logging.getLogger(__name__)

class RESTClient(object):
    def __init__(self, https=False, **kwargs) -> None:
        super().__init__()
        url = kwargs.get('url')
        url_parts_defined = [arg for arg in ['host', 'port', 'endpoint'] if arg in kwargs]

        if url is None and not any(url_parts_defined):
            url = os.environ.get(UBII_SERVICE_URL)
            if not url:
                raise ValueError(f"When creating the REST Client no arguments where passed to define the URL and Environment Variable {UBII_SERVICE_URL} is missing.")

        if url and any(url_parts_defined):
            raise ValueError(f"When creating the REST Client arguments {','.join(url_parts_defined)} can't be used when also using 'url'.")

        self.https = https
        if url:
            self.url = url if url.startswith('http') else f"http{'s' if self.https else ''}://{url}"
        else:
            self.server = kwargs.get('server', '')
            self.port = kwargs.get('port', '')
            self.endpoint = kwargs.get('endpoint', '')
            self.url = f"http{'s' if self.https else ''}://{self.server}:{self.port}/{self.endpoint}"

        from ..session import Session
        self.session = Session.get()

    async def send(self, message):
        async with self.session.client_session.post(self.url, json=message) as resp:
            resp = await resp.text()
            return resp

    def __str__(self):
        return f"REST Client for {self.url}"

    async def shutdown(self):
        log.info(f"Shutting down {self}")

