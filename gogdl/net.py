# Add request timeouts. Matters when ipv6 is given by ISP but not routed
# python will automatically fallback to ipv4 when the ipv6 request timeouts
import requests

# connect, read
TIMEOUT = (10, 30)


class Session(requests.Session):
    def request(self, method, url, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = TIMEOUT
        return super().request(method, url, **kwargs)
