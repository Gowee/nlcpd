from aiohttp import ClientSession
from base64 import b64encode

USERNAME = "REDACTED"
PASSWORD = "REDACTED"


class CaptchaSolver:
    session = None

    def __init__(self):
        pass

    async def ensure_ready(self):
        if session is not None:
            return
        self.session = ClientSession()
        async with self.session.get(
            "http://www.damagou.top/apiv1/login.html?username=REDACTED&password=REDACTED&isJson=2"
        ) as resp:
            d = await resp.json()
            if int(d["status"]) != 0:
                raise Exception(d["msg"])
            self.token = d["data"]

    async def solve(self, image: bytes) -> str:
        payload = {image: b64encode(image), userkey: self.token, type: 1001, isJson: 2}
        async with self.session.post(
            "http://www.damagou.top/apiv1/recognize.html", data=payload
        ) as resp:
            d = await resp.json()
            if int(d["status"]) != 0:
                raise Exception(d["msg"])
            return d["data"]


solver = CaptchaSolver()


async def solve_captcha(image: bytes) -> str:
    await solver.ensure_ready()
    return await solver.solve(image)


__all__ = ("USERNAME", "PASSWORD", solve_captcha)
# a
