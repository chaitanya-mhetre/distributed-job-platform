from __future__ import annotations

import httpx

from relay import JobContext, Relay
from relay.api import create_app


def client_for(relay: Relay) -> httpx.AsyncClient:
    # lifespan is not run by ASGITransport; the `relay` fixture already called setup().
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(relay)), base_url="http://t"
    )


async def test_submit_get_and_list(relay: Relay) -> None:
    @relay.job("echo")
    async def echo(ctx: JobContext, **kw: object) -> object:
        return kw

    async with client_for(relay) as c:
        r = await c.post("/v1/jobs", json={"type": "echo", "payload": {"x": 1}})
        assert r.status_code == 201, r.text
        job_id = r.json()["id"]
        assert r.json()["status"] == "queued"

        r = await c.get(f"/v1/jobs/{job_id}")
        assert r.status_code == 200 and r.json()["payload"] == {"x": 1}

        r = await c.get("/v1/jobs", params={"status": "queued"})
        assert [j["id"] for j in r.json()["items"]] == [job_id]

        assert (await c.post("/v1/jobs", json={"type": "nope"})).status_code == 422
        assert (await c.get("/v1/jobs/00000000-0000-0000-0000-000000000000")).status_code == 404


async def test_api_key_and_payload_limit(relay: Relay) -> None:
    relay.settings.api_key = "secret"
    relay.settings.max_payload_bytes = 100

    @relay.job("echo")
    async def echo(ctx: JobContext, **kw: object) -> object:
        return kw

    async with client_for(relay) as c:
        assert (await c.post("/v1/jobs", json={"type": "echo"})).status_code == 401
        h = {"X-API-Key": "secret"}
        assert (await c.post("/v1/jobs", json={"type": "echo"}, headers=h)).status_code == 201
        big = {"type": "echo", "payload": {"x": "a" * 500}}
        assert (await c.post("/v1/jobs", json=big, headers=h)).status_code == 413


async def test_pagination(relay: Relay) -> None:
    @relay.job("echo")
    async def echo(ctx: JobContext, **kw: object) -> object:
        return kw

    for i in range(5):
        await relay.enqueue("echo", {"i": i})
    async with client_for(relay) as c:
        seen: list[str] = []
        cursor = None
        while True:
            params: dict[str, str | int] = {"limit": 2}
            if cursor:
                params["cursor"] = cursor
            page = (await c.get("/v1/jobs", params=params)).json()
            seen += [j["id"] for j in page["items"]]
            cursor = page["next_cursor"]
            if not cursor:
                break
        assert len(seen) == len(set(seen)) == 5
