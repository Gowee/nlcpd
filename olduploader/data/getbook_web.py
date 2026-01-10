#!/usr/bin/env python3
#(VIBED)
import asyncio
import re
from aiohttp import web, ClientSession, ClientTimeout

BASE = "http://read.nlc.cn"
DOC_HOSTS = [f"{BASE}/doc{i}" for i in (1, 2, 3)]

DECHUNK_SIZE = 1024 * 1024 * 1024  # 1 GiB (kept for parity)

RE_TITLE = re.compile(r"var title = \'(.*?)\'")
RE_PDFNAME = re.compile(r"var pdfname\s*=\s*\'([^']+)\'")
RE_TOKEN = re.compile(r'tokenKey=\"(\w+)\"')
RE_KIME = re.compile(r'timeKey=\"(\w+)\"')
RE_FIME = re.compile(r'timeFlag=\"(\w+)\"')

HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def filtered_headers(headers):
    return {
        k: v for k, v in headers.items()
        if k.lower() not in HOP_HEADERS
    }


async def proxy_stream(request, upstream, extra_headers=None):
    headers = filtered_headers(upstream.headers)
    if extra_headers:
        headers.update(extra_headers)

    resp = web.StreamResponse(
        status=upstream.status,
        headers=headers,
    )
    await resp.prepare(request)

    async for chunk in upstream.content.iter_chunked(64 * 1024):
        await resp.write(chunk)

    await resp.write_eof()
    return resp


async def fetch_path(session, request, file_path: str):
    # HEAD probe doc1/2/3
    head_resps = await asyncio.gather(
        *[
            session.head(
                f"{host}/{file_path}",
                headers=filtered_headers(request.headers),
            )
            for host in DOC_HOSTS
        ],
        return_exceptions=True,
    )

    eff = next(
        (r for r in head_resps if hasattr(r, "status") and r.status == 200),
        None,
    )
    if not eff:
        raise web.HTTPNotFound(text="File not found on any doc host")

    url = str(eff.url)

    headers = filtered_headers(request.headers)
    upstream = await session.get(url, headers=headers)

    return await proxy_stream(
        request,
        upstream,
        {
            "x-orig-url": url,
            "x-stream-type": "direct",
        },
    )


async def get_book_info(session, aid, bid):
    url = f"{BASE}/OutOpenBook/OpenObjectBook?aid={aid}&bid={bid}.0"
    async with session.get(url) as r:
        text = await r.text()

    if "系统内部错误" in text:
        raise web.HTTPBadGateway(text="Upstream internal error")

    try:
        title = RE_TITLE.search(text).group(1)
        pdfname = RE_PDFNAME.search(text).group(1)
        token = RE_TOKEN.search(text).group(1)
        kime = RE_KIME.search(text).group(1)
        fime = RE_FIME.search(text).group(1)
    except AttributeError:
        raise web.HTTPBadRequest(text="Failed to parse OpenObjectBook response")

    return title, pdfname, token, kime, fime


async def handle(request):
    timeout = ClientTimeout(total=None)
    async with ClientSession(timeout=timeout) as session:
        params = request.rel_url.query
        file_path = params.get("file_path")

        # Mode 1: direct path
        if file_path:
            return await fetch_path(session, request, file_path)

        # Mode 2: /getbook/{aid}/{bid}
        parts = request.path.split("/")
        if len(parts) < 4:
            raise web.HTTPForbidden(text="Invalid arguments")

        _, _, aid, bid = parts[:4]

        title, pdfname, token, kime, fime = await get_book_info(session, aid, bid)

        # Prefer doc host streaming (same as Workers)
        try:
            return await fetch_path(session, request, pdfname)
        except web.HTTPException:
            pass

        # Fallback: getReader POST
        url = (
            f"{BASE}/menhu/OutOpenBook/getReader"
            f"?aid={aid}&bid={bid}&kime={kime}&fime={fime}"
        )

        headers = filtered_headers(request.headers)
        headers["myreader"] = token

        upstream = await session.post(url, headers=headers)

        return await proxy_stream(
            request,
            upstream,
            {"x-stream-type": "reader"},
        )


def create_app():
    app = web.Application()
    app.router.add_route("*", "/getbook", handle)
    app.router.add_route("*", "/getbook/{aid}/{bid}", handle)
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=8080)

