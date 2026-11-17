"""选点（/pick）与回报（/reports）端到端自测。

不依赖 Docker，直接驱动本地三个进程（source + 两个共享 SQLite 的 cache 实例）。
测试参数：ISOLATION_SECONDS=2、PROBE_TTL=1（默认生产值为 10/5，由环境变量覆盖）。

服务：
  payments（3 节点）：常规轮转、隔离避开、全部隔离 503、试探优先与恢复、修订推进
  solo（1 节点）    ：试探失败续隔离、超时重获资格、迟到回报、并发抢试探唯一性

覆盖：
P1  常规选点与轮转、跨实例共享、凭据唯一
P2  failure 回报隔离；隔离期内两实例都避开；全部隔离明确 503
P3  隔离期满 -> 两实例合计最多一个试探；试探在途 503；成功恢复常规选点
P4  试探失败 -> 继续隔离，选点仍 503（单节点）
P5  试探无人回报超时 -> 立即重新取得资格；旧试探迟到回报不改写新试探结果
P6  同一凭据重复回报幂等（不重复改变状态）
P7  旧修订的迟到回报不影响新修订；未知凭据 404
P8  并发抢试探：20 个并发请求合计最多 1 个 probe（单节点）
P9  /docs 与 OpenAPI 暴露选点与回报接口
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
DATA = ROOT / "data"
LOG = DATA / "logs"
SRC, C1, C2 = "http://127.0.0.1:8080", "http://127.0.0.1:8081", "http://127.0.0.1:8082"
SERVICE = "payments"
SOLO = "solo"
NODES = ["10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80"]
ISO, PROBE = 2.05, 1.15  # 略大于隔离窗口(2s)/试探 TTL(1s) 的等待量

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((PASS if ok else FAIL, name, detail))
    print(f"[{PASS if ok else FAIL}] {name} {detail}")


class Stack:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen] = []

    def start(self, what: str, port: int, env: dict[str, str]) -> subprocess.Popen:
        log = open(LOG / f"pick_{what}_{port}.log", "ab")
        p = subprocess.Popen(
            [PY, "-m", "uvicorn", f"app.{what}:app", "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(ROOT),
            env={**os.environ, **env},
            stdout=log,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
        self.procs.append(p)
        return p

    def stop_all(self) -> None:
        for p in self.procs:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass


async def wait_ready(client: httpx.AsyncClient, urls: list[str], timeout: float = 20) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = True
        for u in urls:
            try:
                if (await client.get(u)).status_code != 200:
                    ok = False
            except Exception:
                ok = False
        if ok:
            return
        await asyncio.sleep(0.2)
    raise RuntimeError("services did not become ready")


async def node_states(client: httpx.AsyncClient, service: str = SERVICE) -> dict[str, dict]:
    body = (await client.get(f"{C1}/debug/state")).json()
    return {r["node"]: r for r in body["node_states"] if r["service"] == service}


async def pick(client: httpx.AsyncClient, service: str, base: str = C1) -> httpx.Response:
    return await client.post(f"{base}/pick/{service}")


async def report(client: httpx.AsyncClient, token: str, outcome: str) -> httpx.Response:
    return await client.post(f"{C1}/reports", json={"token": token, "outcome": outcome})


async def isolate_node(client: httpx.AsyncClient, service: str, node: str) -> None:
    """通过选点+failure 回报把指定 healthy 节点隔离（带界重试，全程远小于隔离窗口）。"""
    for _ in range(40):
        rp = await pick(client, service)
        assert rp.status_code == 200, rp.text
        body = rp.json()
        if body["node"] == node and body["kind"] == "normal":
            rr = await report(client, body["token"], "failure")
            assert rr.status_code == 200 and rr.json()["applied"] is True, rr.text
            return
        await report(client, body["token"], "success")
    raise AssertionError(f"node {node} never came up for isolation")


async def main() -> int:
    DATA.mkdir(exist_ok=True)
    LOG.mkdir(parents=True, exist_ok=True)
    for f in (DATA / "pick_source.db", DATA / "pick_cache.db"):
        for suffix in ("", "-wal", "-shm"):
            Path(str(f) + suffix).unlink(missing_ok=True)

    stack = Stack()
    base_env = {
        "SOURCE_DB": str(DATA / "pick_source.db"),
        "CACHE_DB": str(DATA / "pick_cache.db"),
        "ISOLATION_SECONDS": "2",
        "PROBE_TTL": "1",
        "REPORT_TTL": "30",
    }
    stack.start("source", 8080, base_env)
    stack.start("cache", 8081, {**base_env, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache1"})
    stack.start("cache", 8082, {**base_env, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache2"})

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            await wait_ready(client, [f"{SRC}/docs", f"{C1}/health", f"{C2}/health"])
            await client.post(f"{C1}/debug/reset")
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": NODES})
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SOLO, "nodes": ["solo-1:80"]})
            assert (await client.get(f"{C1}/routes/{SERVICE}")).status_code == 200
            assert (await client.get(f"{C1}/routes/{SOLO}")).status_code == 200

            # ------------------------------------------------ P1 常规选点 + 轮转
            picks = []
            for base in (C1, C2, C1):
                rp = await pick(client, SERVICE, base)
                assert rp.status_code == 200, rp.text
                picks.append(rp.json())
            ok_p1 = (
                all(p["kind"] == "normal" and p["probe"] is False for p in picks)
                and {p["node"] for p in picks} == set(NODES)
                and len({p["token"] for p in picks}) == 3
                and all(p["revision"] == 1 for p in picks)
            )
            record("P1 常规选点跨实例轮转且凭据唯一", ok_p1,
                   f"nodes={[p['node'] for p in picks]} kinds={[p['kind'] for p in picks]}")

            # ------------------------------------------------ P2 失败隔离 + 503
            node1 = picks[0]["node"]
            rr = await report(client, picks[0]["token"], "failure")
            st = await node_states(client)
            ok_p2a = (
                rr.status_code == 200 and rr.json()["applied"] is True
                and rr.json()["reason"] == "applied"
                and st[node1]["state"] == "isolated"
            )
            record("P2a failure 回报使节点进入隔离", ok_p2a,
                   f"node={node1} state={rr.json().get('state', {}).get('state')}")

            seen = set()
            for _ in range(6):
                rp = await pick(client, SERVICE, C2)
                seen.add(rp.json()["node"])
            ok_p2b = node1 not in seen and seen.issubset(set(NODES) - {node1})
            record("P2b 隔离期内两实例选点均避开故障节点", ok_p2b, f"seen={sorted(seen)}")

            for n in set(NODES) - {node1}:
                await isolate_node(client, SERVICE, n)
            r503 = await pick(client, SERVICE)
            detail = r503.json().get("detail", {})
            ok_p2c = (
                r503.status_code == 503
                and len(detail.get("node_states", [])) == len(NODES)
                and all(s["state"] == "isolated" for s in detail["node_states"])
                and detail.get("retry_after") is not None
            )
            record("P2c 无可用节点时明确 503 并附带节点状态", ok_p2c,
                   f"status={r503.status_code} retry_after={detail.get('retry_after')}")

            # ------------------------------------------------ P3 期满唯一试探 + 成功恢复
            # node1 最早隔离：只等它的窗口到期，其余两节点窗口仍关闭 -> 确定性
            await asyncio.sleep(ISO)
            probe_resp = await pick(client, SERVICE, C1)
            pb = probe_resp.json()
            st = await node_states(client)
            ok_p3a = (
                probe_resp.status_code == 200 and pb["kind"] == "probe"
                and pb["probe"] is True and pb["node"] == node1
                and st[node1]["state"] == "probing"
            )
            record("P3a 隔离期满优先于 healthy 节点放行唯一试探", ok_p3a,
                   f"node={pb.get('node')} kind={pb.get('kind')}")

            # 试探 TTL 较短，立即回报成功（多节点场景的恢复路径）
            rr_p = await report(client, pb["token"], "success")
            ok_p3d = (
                rr_p.json()["applied"] is True
                and (await node_states(client))[node1]["state"] == "healthy"
            )
            record("P3d 多节点场景试探成功恢复 healthy", ok_p3d,
                   f"reason={rr_p.json().get('reason')}")

            # 严格"试探在途不重复放行 + 成功恢复常规选点"在单节点服务上验证
            rp = await pick(client, SOLO)
            await report(client, rp.json()["token"], "failure")  # solo 进入隔离
            ok_iso503 = (await pick(client, SOLO)).status_code == 503
            await asyncio.sleep(ISO)
            rp_probe = await pick(client, SOLO)
            r_mid = await pick(client, SOLO, C2)  # 试探在途：必须 503
            ok_p3b = (
                ok_iso503 and rp_probe.json()["kind"] == "probe"
                and r_mid.status_code == 503
            )
            record("P3b 试探在途不重复放行第二个试探（单节点严格验证）", ok_p3b,
                   f"isolated={ok_iso503} probe={rp_probe.json().get('kind')} "
                   f"second_pick={r_mid.status_code}")

            rr = await report(client, rp_probe.json()["token"], "success")
            st_solo = await node_states(client, SOLO)
            after = await pick(client, SOLO, C2)
            ok_p3c = (
                rr.json()["applied"] is True
                and st_solo["solo-1:80"]["state"] == "healthy"
                and after.status_code == 200 and after.json()["kind"] == "normal"
            )
            record("P3c 试探成功恢复常规选点", ok_p3c,
                   f"report_reason={rr.json().get('reason')} next_kind={after.json().get('kind')}")
            await report(client, after.json()["token"], "success")

            # ------------------------------------------------ P4 试探失败继续隔离（单节点）
            rp = await pick(client, SOLO)
            assert rp.json()["kind"] == "normal"
            await report(client, rp.json()["token"], "failure")
            r_no = await pick(client, SOLO)
            ok_p4a = r_no.status_code == 503
            await asyncio.sleep(ISO)
            rp2 = await pick(client, SOLO)
            ok_p4b = rp2.status_code == 200 and rp2.json()["kind"] == "probe"
            rr = await report(client, rp2.json()["token"], "failure")
            st = await node_states(client, SOLO)
            r_iso = await pick(client, SOLO)
            ok_p4 = (
                ok_p4a and ok_p4b and rr.json()["applied"] is True
                and st["solo-1:80"]["state"] == "isolated" and r_iso.status_code == 503
            )
            record("P4 试探失败继续隔离，单节点选点明确 503", ok_p4,
                   f"isolated_503={r_no.status_code} probe={rp2.json().get('kind')} "
                   f"after_fail={r_iso.status_code}")

            # ------------------------------------------------ P5 超时重获资格 + 旧试探迟到回报
            await asyncio.sleep(ISO)  # 隔离窗口期满
            rp3 = await pick(client, SOLO)
            old_token = rp3.json()["token"]
            epoch_old = (await node_states(client, SOLO))["solo-1:80"]["probe_epoch"]
            ok_p5a = rp3.json()["kind"] == "probe"
            await asyncio.sleep(PROBE)  # 试探超时无人回报（懒回收：下次选点才处理）
            st = await node_states(client, SOLO)
            ok_p5b = (
                st["solo-1:80"]["state"] == "probing"
                and st["solo-1:80"]["probe_expires_at"] < time.time()
            )

            # 下次选点触发懒回收并立即重新取得资格：同一请求内完成回收+新试探
            rp4 = await pick(client, SOLO)
            st_after_pick = (await node_states(client, SOLO))["solo-1:80"]
            epoch_new = st_after_pick["probe_epoch"]
            ok_p5c = (
                rp4.status_code == 200 and rp4.json()["kind"] == "probe"
                and epoch_new > epoch_old
                and st_after_pick["state"] == "probing"
            )

            rr_late = await report(client, old_token, "success")  # 旧试探迟到回报
            st_after = (await node_states(client, SOLO))["solo-1:80"]
            ok_p5d = (
                rr_late.json()["applied"] is False
                and rr_late.json()["reason"] == "stale"
                and st_after["state"] == "probing"  # 新试探结果未被旧回报改写
                and st_after["probe_epoch"] == epoch_new
            )
            rr_new = await report(client, rp4.json()["token"], "success")
            st_final = (await node_states(client, SOLO))["solo-1:80"]
            ok_p5e = rr_new.json()["applied"] is True and st_final["state"] == "healthy"
            record("P5 试探超时懒回收、立即重获资格且旧试探迟到回报不改写新试探",
                   ok_p5a and ok_p5b and ok_p5c and ok_p5d and ok_p5e,
                   f"epoch {epoch_old}->{epoch_new} late={rr_late.json().get('reason')} "
                   f"new={rr_new.json().get('reason')}")

            # ------------------------------------------------ P6 重复回报幂等（单节点）
            rp = await pick(client, SOLO)
            t, n = rp.json()["token"], rp.json()["node"]
            r1 = await report(client, t, "failure")
            until1 = (await node_states(client, SOLO))[n]["isolated_until"]
            await asyncio.sleep(0.2)
            r2 = await report(client, t, "failure")
            r3 = await report(client, t, "success")  # 同凭据、相反结果
            st2 = (await node_states(client, SOLO))[n]
            ok_p6 = (
                r1.json()["applied"] is True
                and r2.json()["reason"] == "duplicate" and r2.json()["applied"] is False
                and r3.json()["reason"] == "duplicate" and r3.json()["applied"] is False
                and st2["state"] == "isolated" and st2["isolated_until"] == until1
            )
            record("P6 同一凭据重复回报不重复改变状态", ok_p6,
                   f"r1={r1.json()['reason']} r2={r2.json()['reason']} r3={r3.json()['reason']}")

            # ------------------------------------------------ P7 旧修订迟到回报
            # 取一个 rev1 凭据且先不回报（此刻 payments 上可能是 normal 或 probe，均可）
            rp = await pick(client, SERVICE)
            old_rev_token, old_rev = rp.json()["token"], rp.json()["revision"]
            new_nodes = ["10.0.1.1:80", "10.0.1.2:80"]
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": new_nodes})
            rnew = await pick(client, SERVICE)
            ok_p7a = (
                rnew.status_code == 200 and rnew.json()["revision"] == old_rev + 1
                and rnew.json()["node"] in new_nodes
            )
            rr = await report(client, old_rev_token, "failure")  # 旧修订迟到回报
            picked_after = {(await pick(client, SERVICE)).json()["node"] for _ in range(4)}
            st = await node_states(client)
            new_healthy = all(st[n]["state"] == "healthy" for n in new_nodes)
            ok_p7b = (
                rr.json()["applied"] is False and rr.json()["reason"] == "stale_revision"
                and picked_after.issubset(set(new_nodes)) and new_healthy
            )
            record("P7 旧修订迟到回报不影响新修订选点", ok_p7a and ok_p7b,
                   f"old_rev={old_rev} reason={rr.json().get('reason')} "
                   f"picked={sorted(picked_after)}")

            r404 = await report(client, "deadbeef" * 4, "success")
            record("P7b 未知凭据明确 404", r404.status_code == 404, f"status={r404.status_code}")

            # ------------------------------------------------ P8 并发抢试探唯一（单节点）
            # P6 后节点处于 isolated；等隔离窗口期满，20 个并发请求同时抢唯一试探
            await asyncio.sleep(ISO)
            responses = await asyncio.gather(*[
                pick(client, SOLO, C1 if i % 2 else C2) for i in range(20)
            ])
            codes = sorted({r.status_code for r in responses})
            probes = [r.json() for r in responses if r.status_code == 200]
            ok_p8 = (
                set(codes).issubset({200, 503})
                and len(probes) == 1 and probes[0]["kind"] == "probe"
                and len({p["token"] for p in probes}) == 1
            )
            record("P8 20 并发抢试探：两实例合计仅 1 个 probe", ok_p8,
                   f"codes={codes} probes={len(probes)}")
            await report(client, probes[0]["token"], "success")

            # ------------------------------------------------ P9 /docs 与 OpenAPI
            for name, url in [("cache1 /docs", f"{C1}/docs"), ("cache2 /docs", f"{C2}/docs")]:
                rd = await client.get(url)
                record(f"P9 {name} 可打开", rd.status_code == 200, f"status={rd.status_code}")
            spec = (await client.get(f"{C1}/openapi.json")).json()
            ok_api = "/pick/{service}" in spec["paths"] and "/reports" in spec["paths"]
            record("P9b OpenAPI 暴露选点与回报接口", ok_api,
                   f"paths={[p for p in spec['paths'] if 'pick' in p or 'report' in p]}")

    finally:
        stack.stop_all()

    failed = [r for r in results if r[0] == FAIL]
    print("\n================ 选点自测汇总 ================")
    for st, name, detail in results:
        print(f"  [{st}] {name} {detail}")
    print(f"\n通过 {len(results) - len(failed)}/{len(results)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
