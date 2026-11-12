from aiohttp.test_utils import TestClient, TestServer

from kvstore.config import DEFAULT_CONFIG, load_config
from kvstore.server import build_app


def test_config_loads():
    cfg = load_config()
    assert cfg.n == 3 and len(cfg.nodes) == 5
    assert cfg.conflict_policy in ("siblings", "lww")


async def test_health_endpoint():
    async with TestClient(TestServer(build_app("n1", str(DEFAULT_CONFIG)))) as client:
        resp = await client.get("/health")
        assert resp.status == 200
        assert await resp.json() == {"node": "n1", "ok": True}
        # Placeholder until Person B implements the client API:
        assert (await client.get("/kv/x")).status in (200, 501)
