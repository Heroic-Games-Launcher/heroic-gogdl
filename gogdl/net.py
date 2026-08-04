# requests has no timeout by default. Matters when ipv6 is advertised but not
# routed: the connect stalls and we only get to the ipv4 address once it times out
import requests

# connect, read
TIMEOUT = (10, 30)


class Session(requests.Session):
    def request(self, method, url, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = TIMEOUT
        return super().request(method, url, **kwargs)
