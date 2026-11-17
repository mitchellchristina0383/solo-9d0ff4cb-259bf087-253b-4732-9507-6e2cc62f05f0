"""路由查询缓存实例（cache）。

两个实例共享同一个 SQLite 数据库（docker volume / 本地共享目录），通过
SQLite 的行级事务协调"跨实例并发未命中合并（singleflight）"。

关键保证：
1. 修订单调：每次查询先向配置源确认当前修订；新修订提交后发起的查询
   不可能返回旧修订的节点。
2. 同修订并发未命中合并：同一 (service, revision) 同一时刻只允许一个实例
   回源，其余实例在库内等待结果（snapshot 或 failure 记录）。
3. 租约接管：回源持有者周期性续租；持有者超时或进程退出后，其他实例
   可在租约过期后接管回源。
4. 慢回源跨修订不写脏数据：回源返回后再次核对"该修订仍是当前修订"，
   过期修订只标记为 stale，绝不作为有效缓存。
5. 回源失败明确 503：失败记录带冷却期，冷却期内快速失败；旧快照
   仍可通过 /snapshots 与故障响应中的 last_good 排障。
6. 故障节点选点隔离（/pick + /reports）：失败节点隔离 10 秒；隔离期满后
   两个缓存实例合计最多放行一个试探请求，试探成功才恢复常规选点，失败则
   继续隔离；试探无人回报而超时也会重新取得资格。凭据（token）一次性、
   试探带代次（epoch），迟到回报/重复回报/旧修订回报都不能改写当前状态；
   没有可选节点时明确返回 503。
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import uuid
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DB_PATH = os.environ.get("CACHE_DB", "/data/cache.db")
SOURCE_BASE = os.environ.get("SOURCE_BASE", "http://source:8080")
INSTANCE_ID = os.environ.get("INSTANCE_ID", f"cache-{uuid.uuid4().hex[:8]}")
HTTP_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "6.0"))
LEASE_TTL = float(os.environ.get("LEASE_TTL", "2.0"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "0.7"))
FAILURE_COOLDOWN = float(os.environ.get("FAILURE_COOLDOWN", "3.0"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "0.15"))
# 选点隔离：失败节点隔离时长；隔离期满后允许一次跨实例唯一的试探
ISOLATION_SECONDS = float(os.environ.get("ISOLATION_SECONDS", "10.0"))
# 试探凭据有效期：无人回报时，期满后节点重新取得试探资格
PROBE_TTL = float(os.environ.get("PROBE_TTL", "5.0"))


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = _connect()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            service     TEXT NOT NULL,
            revision    INTEGER NOT NULL,
            nodes       TEXT NOT NULL,
            status      TEXT NOT NULL,            -- valid | stale
            source_published_at REAL,
            cached_at   REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inflight (
            service     TEXT NOT NULL,
            revision    INTEGER NOT NULL,
            leader      TEXT NOT NULL,
            lease_expires_at REAL NOT NULL,
            started_at  REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS failures (
            service    TEXT NOT NULL,
            revision   INTEGER NOT NULL,
            error      TEXT NOT NULL,
            cooldown_until REAL NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    # 选点隔离状态：按 (service, revision, node) 分行。
    # 新修订使用全新的一组行，旧修订的迟到回报天然无法触及新修订状态。
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS node_states (
            service    TEXT NOT NULL,
            revision   INTEGER NOT NULL,
            node       TEXT NOT NULL,
            state      TEXT NOT NULL,            -- healthy | isolated | probing
            isolated_until  REAL,                -- isolated: 隔离期满时刻
            probe_expires_at REAL,               -- probing: 试探超时时刻
            probe_epoch INTEGER NOT NULL DEFAULT 0,  -- 每次发起试探 +1，用于识别旧试探迟到回报
            updated_at REAL NOT NULL,
            PRIMARY KEY (service, revision, node)
        )
        """
    )
    # 回报凭据：一次性。重复回报时 token 已 reported，不再改变节点状态。
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pick_tokens (
            token      TEXT PRIMARY KEY,
            service    TEXT NOT NULL,
            revision   INTEGER NOT NULL,
            node       TEXT NOT NULL,
            kind       TEXT NOT NULL,            -- normal | probe
            probe_epoch INTEGER NOT NULL,        -- 发凭据时节点的试探代次
            issued_by  TEXT NOT NULL,
            issued_at  REAL NOT NULL,
            expires_at REAL NOT NULL,
            reported   INTEGER NOT NULL DEFAULT 0,
            outcome    TEXT,                     -- success | failure（回报后落档）
            reported_at REAL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pick_tokens_svc ON pick_tokens(service, revision)"
    )
    # 常规选点的轮转游标（按修订快照的节点排序取模，保证多实例下均匀）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pick_cursors (
            service  TEXT NOT NULL,
            revision INTEGER NOT NULL,
            counter  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.close()


# ---------------------------------------------------------------------------
# 配置源客户端
# ---------------------------------------------------------------------------
class SourceUnavailable(Exception):
    pass


async def _source_current(service: str) -> int:
    try:
        async with httpx.AsyncClient(base_url=SOURCE_BASE, timeout=HTTP_TIMEOUT) as c:
            r = await c.get(f"/services/{service}/current")
            if r.status_code == 404:
                raise HTTPException(status_code=404, detail=f"service '{service}' not found")
            r.raise_for_status()
            return int(r.json()["revision"])
    except HTTPException:
        raise
    except Exception as exc:  # 连不上 / 超时 / 5xx
        raise SourceUnavailable(f"current revision lookup failed: {exc}") from exc


async def _source_fetch(service: str, revision: int) -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=SOURCE_BASE, timeout=HTTP_TIMEOUT) as c:
        r = await c.get(f"/services/{service}/revisions/{revision}")
        if r.status_code >= 400:
            raise SourceUnavailable(f"origin returned HTTP {r.status_code}: {r.text[:200]}")
        return r.json()


# ---------------------------------------------------------------------------
# DB 辅助
# ---------------------------------------------------------------------------
def _get_snapshot(conn: sqlite3.Connection, service: str, revision: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM snapshots WHERE service=? AND revision=?", (service, revision)
    ).fetchone()


def _last_good(conn: sqlite3.Connection, service: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM snapshots WHERE service=? AND status='valid' "
        "ORDER BY revision DESC LIMIT 1",
        (service,),
    ).fetchone()
    if row is None:
        return None
    return {
        "service": row["service"],
        "revision": row["revision"],
        "nodes": json.loads(row["nodes"]),
        "cached_at": row["cached_at"],
    }


def _try_acquire_lease(conn: sqlite3.Connection, service: str, revision: int) -> bool:
    """尝试成为 (service, revision) 的回源 leader；已有未过期租约则失败。"""
    now = time.time()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT leader, lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, revision),
        ).fetchone()
        if row is not None and row["lease_expires_at"] > now:
            conn.commit()
            return False
        # 无记录，或租约已过期 -> 接管
        conn.execute(
            "INSERT INTO inflight(service, revision, leader, lease_expires_at, started_at) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(service, revision) DO UPDATE SET "
            "leader=excluded.leader, lease_expires_at=excluded.lease_expires_at, "
            "started_at=excluded.started_at",
            (service, revision, INSTANCE_ID, now + LEASE_TTL, now),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


def _renew_lease(conn: sqlite3.Connection, service: str, revision: int) -> bool:
    now = time.time()
    row = conn.execute(
        "SELECT leader FROM inflight WHERE service=? AND revision=?", (service, revision)
    ).fetchone()
    if row is None or row["leader"] != INSTANCE_ID:
        return False
    conn.execute(
        "UPDATE inflight SET lease_expires_at=? WHERE service=? AND revision=? AND leader=?",
        (now + LEASE_TTL, service, revision, INSTANCE_ID),
    )
    return True


def _release_lease(conn: sqlite3.Connection, service: str, revision: int) -> None:
    conn.execute(
        "DELETE FROM inflight WHERE service=? AND revision=? AND leader=?",
        (service, revision, INSTANCE_ID),
    )


def _record_failure(conn: sqlite3.Connection, service: str, revision: int, error: str) -> None:
    now = time.time()
    conn.execute(
        "INSERT INTO failures(service, revision, error, cooldown_until, updated_at) "
        "VALUES(?,?,?,?,?) "
        "ON CONFLICT(service, revision) DO UPDATE SET error=excluded.error, "
        "cooldown_until=excluded.cooldown_until, updated_at=excluded.updated_at",
        (service, revision, error[:500], now + FAILURE_COOLDOWN, now),
    )


def _save_snapshot(
    conn: sqlite3.Connection,
    service: str,
    revision: int,
    nodes: list[str],
    source_published_at: float | None,
    still_current: bool,
) -> str:
    """写入快照；仅当修订仍为当前修订时标记 valid，否则标记 stale。"""
    now = time.time()
    status = "valid" if still_current else "stale"
    conn.execute(
        "INSERT INTO snapshots(service, revision, nodes, status, source_published_at, cached_at) "
        "VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(service, revision) DO UPDATE SET nodes=excluded.nodes, "
        "status=excluded.status, source_published_at=excluded.source_published_at, "
        "cached_at=excluded.cached_at",
        (service, revision, json.dumps(nodes), status, source_published_at, now),
    )
    return status


# ---------------------------------------------------------------------------
# 核心：singleflight 查询
# ---------------------------------------------------------------------------
async def _query(service: str) -> dict[str, Any]:
    # 第一步：确认当前修订。配置源整体不可达时降级：
    # 有本地有效快照则降级服务（标记 degraded），无快照才明确 503。
    try:
        current = await _source_current(service)
    except SourceUnavailable:
        conn = _connect()
        try:
            good = _last_good(conn, service)
            if good is None:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "origin unavailable and no local snapshot",
                        "service": service,
                        "last_good": None,
                    },
                )
            return {
                "service": service,
                "revision": good["revision"],
                "nodes": good["nodes"],
                "served_by": INSTANCE_ID,
                "from_cache": True,
                "degraded": True,
                "cached_at": good["cached_at"],
            }
        finally:
            conn.close()

    conn = _connect()
    try:
        snap = _get_snapshot(conn, service, current)
        if snap is not None and snap["status"] == "valid":
            return {
                "service": service,
                "revision": current,
                "nodes": json.loads(snap["nodes"]),
                "served_by": INSTANCE_ID,
                "from_cache": True,
                "cached_at": snap["cached_at"],
            }

        # 失败冷却期内快速明确失败（旧快照仍可单独查询）。
        # 若仍有 leader 持有效租约（其回源可能仍在途），不提前失败，转入等待/接管逻辑。
        fail = conn.execute(
            "SELECT * FROM failures WHERE service=? AND revision=?", (service, current)
        ).fetchone()
        inflight_row = conn.execute(
            "SELECT lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, current),
        ).fetchone()
        lease_alive = inflight_row is not None and inflight_row["lease_expires_at"] > time.time()
        if fail is not None and fail["cooldown_until"] > time.time() and not lease_alive:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "origin unavailable (failure cooldown)",
                    "reason": fail["error"],
                    "retry_after": round(fail["cooldown_until"] - time.time(), 2),
                    "last_good": _last_good(conn, service),
                },
            )

        is_leader = _try_acquire_lease(conn, service, current)

        if is_leader:
            return await _run_as_leader(conn, service, current)
        return await _wait_as_follower(conn, service, current)
    finally:
        conn.close()


async def _run_as_leader(conn: sqlite3.Connection, service: str, revision: int) -> dict[str, Any]:
    heartbeat_task = asyncio.create_task(_heartbeat(service, revision))
    try:
        # 硬时限：回源不能无限占用回源资格；超时后停心跳、放租约，
        # 其他实例可在租约过期后接管（即使本进程仍然存活）。
        payload = await asyncio.wait_for(
            _source_fetch(service, revision), timeout=HTTP_TIMEOUT
        )
    except asyncio.TimeoutError:
        heartbeat_task.cancel()
        conn2 = _connect()
        try:
            conn2.execute("BEGIN IMMEDIATE")
            _record_failure(conn2, service, revision, f"origin fetch timeout after {HTTP_TIMEOUT}s")
            _release_lease(conn2, service, revision)
            conn2.commit()
        finally:
            conn2.close()
        raise HTTPException(
            status_code=503,
            detail={
                "error": "origin unavailable",
                "reason": f"origin fetch timeout after {HTTP_TIMEOUT}s",
                "service": service,
                "requested_revision": revision,
                "last_good": _last_good(conn, service),
            },
        )
    except SourceUnavailable as exc:
        heartbeat_task.cancel()
        conn2 = _connect()
        try:
            conn2.execute("BEGIN IMMEDIATE")
            _record_failure(conn2, service, revision, str(exc))
            _release_lease(conn2, service, revision)
            conn2.commit()
        finally:
            conn2.close()
        raise HTTPException(
            status_code=503,
            detail={
                "error": "origin unavailable",
                "reason": str(exc),
                "service": service,
                "requested_revision": revision,
                "last_good": _last_good(conn, service),
            },
        )
    except Exception as exc:
        heartbeat_task.cancel()
        conn2 = _connect()
        try:
            conn2.execute("BEGIN IMMEDIATE")
            _record_failure(conn2, service, revision, f"unexpected: {exc!r}")
            _release_lease(conn2, service, revision)
            conn2.commit()
        finally:
            conn2.close()
        raise HTTPException(
            status_code=503,
            detail={"error": "origin unavailable", "reason": repr(exc),
                    "last_good": _last_good(conn, service)},
        )
    else:
        heartbeat_task.cancel()

    # 回源成功：核对该修订是否仍是当前修订，防止慢回源把旧响应写成有效缓存
    try:
        latest = await _source_current(service)
    except SourceUnavailable:
        latest = revision  # 当前修订查询可靠；极端情况下不降级，按当前值处理
    still_current = latest == revision

    conn3 = _connect()
    try:
        conn3.execute("BEGIN IMMEDIATE")
        status = _save_snapshot(
            conn3,
            service,
            revision,
            payload["nodes"],
            payload.get("published_at"),
            still_current,
        )
        # 成功后清除失败记录与租约
        conn3.execute(
            "DELETE FROM failures WHERE service=? AND revision=?", (service, revision)
        )
        _release_lease(conn3, service, revision)
        conn3.commit()
    finally:
        conn3.close()

    if not still_current:
        # 旧修订已过期：不把它当有效结果返回，让请求走新修订
        raise HTTPException(
            status_code=409,
            detail={
                "error": "revision advanced during slow origin fetch; retry for latest",
                "fetched_revision": revision,
                "current_revision": latest,
            },
        )

    return {
        "service": service,
        "revision": revision,
        "nodes": payload["nodes"],
        "served_by": INSTANCE_ID,
        "from_cache": False,
        "cached_at": time.time(),
        "snapshot_status": status,
    }


async def _heartbeat(service: str, revision: int) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            conn = _connect()
            try:
                if not _renew_lease(conn, service, revision):
                    return  # 已被接管
            finally:
                conn.close()
        except Exception:
            pass


async def _wait_as_follower(
    conn: sqlite3.Connection, service: str, revision: int
) -> dict[str, Any]:
    # 覆盖：leader 硬超时 + 租约 TTL + 接管者自己的回源时间，再加余量
    deadline = time.time() + HTTP_TIMEOUT * 3 + LEASE_TTL * 2
    while time.time() < deadline:
        await asyncio.sleep(POLL_INTERVAL)

        snap = _get_snapshot(conn, service, revision)
        if snap is not None and snap["status"] == "valid":
            return {
                "service": service,
                "revision": revision,
                "nodes": json.loads(snap["nodes"]),
                "served_by": INSTANCE_ID,
                "from_cache": True,
                "coalesced": True,
                "cached_at": snap["cached_at"],
            }

        # leader 失败：共享失败记录生效，冷却期内明确 503。
        # 但若仍有 leader 持有有效租约（例如其回源挂起、只是客户端先超时），
        # 不能直接失败，应等待租约过期后接管。
        fail = conn.execute(
            "SELECT * FROM failures WHERE service=? AND revision=?", (service, revision)
        ).fetchone()
        inflight_row = conn.execute(
            "SELECT lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, revision),
        ).fetchone()
        lease_alive = inflight_row is not None and inflight_row["lease_expires_at"] > time.time()
        if fail is not None and not lease_alive:
            now = time.time()
            if fail["cooldown_until"] > now:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "origin unavailable (coalesced leader failure)",
                        "reason": fail["error"],
                        "retry_after": round(fail["cooldown_until"] - now, 2),
                        "last_good": _last_good(conn, service),
                    },
                )
            # 冷却已过：清掉旧失败记录，尝试接管重新回源
            conn.execute(
                "DELETE FROM failures WHERE service=? AND revision=?", (service, revision)
            )
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(conn, service, revision)

        # 租约过期（leader 超时/退出）：接管
        if inflight_row is None:
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(conn, service, revision)
        elif inflight_row["lease_expires_at"] <= time.time():
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(conn, service, revision)

    raise HTTPException(
        status_code=503,
        detail={"error": "timed out waiting for in-flight origin fetch",
                "last_good": _last_good(conn, service)},
    )


# ---------------------------------------------------------------------------
# 选点与回报：故障节点隔离 + 跨实例唯一试探（half-open）
#
# 节点状态机（每个 (service, revision, node) 一行）：
#   healthy --failure 回报--> isolated --隔离期满, 首个选点请求--> probing
#   probing --success 回报--> healthy（恢复常规选点）
#   probing --failure 回报--> isolated（继续隔离，刷新窗口）
#   probing --超时无人回报--> isolated（重新取得下一轮试探资格）
#
# 并发与陈旧回报防护：
#   - 选点在 BEGIN IMMEDIATE 事务内做条件 UPDATE，两个实例共享同一写锁，
#     同一节点隔离期满后合计只有一个请求能把它转成 probing（最多一个试探）。
#   - 每次发起试探 probe_epoch+1；回报凭据记录发凭据时的 epoch，
#     旧试探的迟到回报 epoch 不符，绝不改写当前状态。
#   - 凭据一次性：重复回报时 token 已 reported，直接返回 duplicate 且不改状态。
#   - 节点状态按修订分行：旧修订的迟到回报只作用于旧修订行，新修订不受影响。
# ---------------------------------------------------------------------------
REPORT_TTL = float(os.environ.get("REPORT_TTL", "30.0"))
# 过期凭据保留宽限期：期内的迟到回报仍可被识别为陈旧回报（applied=false），
# 而不是与"从未签发的伪造凭据"混为 404；超宽限才物理清理，限制表增长。
TOKEN_GRACE = float(os.environ.get("TOKEN_GRACE", "300.0"))


def _expire_probes(conn: sqlite3.Connection, service: str, revision: int, now: float) -> None:
    """超时无人回报的试探：退回 isolated 并立即重新取得试探资格。

    isolated_until 直接取试探的过期时刻（已在过去）：下次选点即可再次放行试探，
    不需要再等一个完整隔离窗口；失败回报造成的隔离仍使用完整 ISOLATION_SECONDS。
    """
    conn.execute(
        "UPDATE node_states SET state='isolated', isolated_until=probe_expires_at, "
        "probe_expires_at=NULL, updated_at=? "
        "WHERE service=? AND revision=? AND state='probing' AND probe_expires_at<=?",
        (now, service, revision, now),
    )


def _ensure_node_rows(
    conn: sqlite3.Connection, service: str, revision: int, nodes: list[str], now: float
) -> None:
    """为修订快照中的节点补齐状态行；已存在的行（含隔离状态）绝不覆盖。"""
    for node in nodes:
        conn.execute(
            "INSERT INTO node_states(service, revision, node, state, updated_at) "
            "VALUES(?,?,?, 'healthy', ?) "
            "ON CONFLICT(service, revision, node) DO NOTHING",
            (service, revision, node, now),
        )


def _node_state_payload(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [
        {
            "node": r["node"],
            "state": r["state"],
            "isolated_until": r["isolated_until"],
            "probe_expires_at": r["probe_expires_at"],
            "probe_epoch": r["probe_epoch"],
        }
        for r in rows
    ]


async def _pick(service: str) -> dict[str, Any]:
    # 复用既有路由查询：自动获得修订单调、降级（degraded）与明确 503/404 行为
    route = await _query(service)
    revision = int(route["revision"])
    nodes = list(route["nodes"])
    now = time.time()

    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        # 物理清理超出宽限期的旧凭据；宽限期内的过期凭据仍可识别陈旧回报
        conn.execute("DELETE FROM pick_tokens WHERE expires_at<=?", (now - TOKEN_GRACE,))
        _ensure_node_rows(conn, service, revision, nodes, now)
        _expire_probes(conn, service, revision, now)

        # 选点优先级：隔离期满可试探的节点 > healthy 常规节点 > 503。
        # 试探优先保证短暂失败的节点不会因其他节点一直 healthy 而永久失去恢复机会；
        # 同一节点的试探在途时不会被重复放行（条件 UPDATE + 循环内重读）。
        chosen: sqlite3.Row | None = None
        kind = ""
        while True:
            rows = conn.execute(
                "SELECT * FROM node_states WHERE service=? AND revision=?",
                (service, revision),
            ).fetchall()
            healthy = [r for r in rows if r["state"] == "healthy"]
            probeable = [
                r for r in rows
                if r["state"] == "isolated" and r["isolated_until"] is not None
                and r["isolated_until"] <= now
            ]

            if probeable:
                # half-open：隔离期满，两实例合计每节点最多放行一个试探。
                # 条件 UPDATE 行级竞争：只有一个事务能把该行转成 probing。
                candidate = sorted(
                    probeable, key=lambda r: (r["isolated_until"], r["node"])
                )[0]
                cur = conn.execute(
                    "SELECT * FROM node_states WHERE service=? AND revision=? AND node=?",
                    (service, revision, candidate["node"]),
                ).fetchone()
                next_epoch = int(cur["probe_epoch"]) + 1
                probe_deadline = now + PROBE_TTL
                conn.execute(
                    "UPDATE node_states SET state='probing', probe_expires_at=?, "
                    "probe_epoch=?, isolated_until=NULL, updated_at=? "
                    "WHERE service=? AND revision=? AND node=? AND state='isolated' "
                    "AND isolated_until<=?",
                    (probe_deadline, next_epoch, now,
                     service, revision, candidate["node"], now),
                )
                if conn.execute("SELECT changes() AS n").fetchone()["n"] == 1:
                    chosen = conn.execute(
                        "SELECT * FROM node_states WHERE service=? AND revision=? AND node=?",
                        (service, revision, candidate["node"]),
                    ).fetchone()
                    kind = "probe"
                    break
                # 抢输（另一实例刚放行该节点的试探）：循环重读，尝试下一候选
                continue

            if healthy:
                # 常规选点：按快照节点排序的轮转游标均匀分发
                cur = conn.execute(
                    "SELECT counter FROM pick_cursors WHERE service=? AND revision=?",
                    (service, revision),
                ).fetchone()
                counter = int(cur["counter"]) if cur else 0
                ordered = sorted(healthy, key=lambda r: r["node"])
                chosen = ordered[counter % len(ordered)]
                conn.execute(
                    "INSERT INTO pick_cursors(service, revision, counter) VALUES(?,?,1) "
                    "ON CONFLICT(service, revision) DO UPDATE SET counter=counter+1",
                    (service, revision),
                )
                kind = "normal"
                break

            # 全部隔离中或试探在途：没有可选节点
            states = _node_state_payload(rows)
            wait_times = [
                (r["isolated_until"] - now)
                for r in rows
                if r["state"] == "isolated" and r["isolated_until"] is not None
                and r["isolated_until"] > now
            ] + [
                (r["probe_expires_at"] - now)
                for r in rows
                if r["state"] == "probing" and r["probe_expires_at"] is not None
                and r["probe_expires_at"] > now
            ]
            retry_after = round(max(min(wait_times), 0.0), 2) if wait_times else 0.0
            conn.rollback()
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "no eligible node: all nodes isolated or under probe",
                    "service": service,
                    "revision": revision,
                    "retry_after": retry_after,
                    "node_states": states,
                },
            )

        token = uuid.uuid4().hex
        token_expiry = (
            float(chosen["probe_expires_at"]) if kind == "probe" else now + REPORT_TTL
        )
        conn.execute(
            "INSERT INTO pick_tokens(token, service, revision, node, kind, probe_epoch, "
            "issued_by, issued_at, expires_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                token, service, revision, chosen["node"], kind,
                int(chosen["probe_epoch"]), INSTANCE_ID, now, token_expiry,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "service": service,
        "revision": revision,
        "node": chosen["node"],
        "token": token,
        "kind": kind,
        "probe": kind == "probe",
        "isolation_seconds": ISOLATION_SECONDS,
        "served_by": INSTANCE_ID,
        "issued_at": now,
        "expires_at": token_expiry,
    }


async def _report(token: str, outcome: str) -> dict[str, Any]:
    now = time.time()
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT * FROM pick_tokens WHERE token=?", (token,)
        ).fetchone()
        if row is None:
            # 仅伪造凭据或已超出宽限期被物理清理的凭据才到这里
            raise HTTPException(
                status_code=404,
                detail={"error": "unknown token", "token": token},
            )

        # 幂等：同一凭据重复回报不再改变任何状态
        if row["reported"]:
            return {
                "applied": False,
                "reason": "duplicate",
                "service": row["service"],
                "revision": row["revision"],
                "node": row["node"],
                "outcome": row["outcome"],
                "state": _current_node_state(
                    conn, row["service"], row["revision"], row["node"]
                ),
            }

        # 凭据已过有效期（试探窗口关闭、节点状态已推进）：迟到回报一律视为陈旧
        token_expired = row["expires_at"] <= now

        # 尽力核对修订：源不可达不影响回报（结构上新旧修订也是分行的）
        stale_revision = False
        try:
            current = await _source_current(row["service"])
            stale_revision = current != row["revision"]
        except (SourceUnavailable, HTTPException):
            stale_revision = False

        conn.execute("BEGIN IMMEDIATE")
        # 事务内重读，防止与并发回报交错
        row = conn.execute(
            "SELECT * FROM pick_tokens WHERE token=?", (token,)
        ).fetchone()
        if row is None:
            conn.rollback()
            raise HTTPException(status_code=404, detail={"error": "token vanished"})
        if row["reported"]:
            conn.rollback()
            return {
                "applied": False,
                "reason": "duplicate",
                "service": row["service"],
                "revision": row["revision"],
                "node": row["node"],
                "outcome": row["outcome"],
                "state": _current_node_state(
                    conn, row["service"], row["revision"], row["node"]
                ),
            }

        reason = "applied"
        applied = False
        node_row = conn.execute(
            "SELECT * FROM node_states WHERE service=? AND revision=? AND node=?",
            (row["service"], row["revision"], row["node"]),
        ).fetchone()

        # 凭据与当前节点状态一致才允许落状态：
        #   probe 凭据：节点须仍处于同一代次的 probing（未超时、未被新试探取代）
        #   normal 凭据：节点须仍为 healthy 且代次未变（未被其他回报隔离）
        expected_state = "probing" if row["kind"] == "probe" else "healthy"
        if stale_revision:
            # 旧修订的迟到回报：只销票，绝不触碰新修订状态
            reason = "stale_revision"
        elif (
            token_expired
            or node_row is None
            or int(node_row["probe_epoch"]) != int(row["probe_epoch"])
            or node_row["state"] != expected_state
        ):
            # 凭据过期 / 旧试探 / 超时试探 / 状态已推进后的迟到回报：不改写当前结果
            reason = "stale"
        else:
            applied = True
            if outcome == "success":
                # 试探成功才恢复常规选点；常规节点的成功回报保持 healthy
                conn.execute(
                    "UPDATE node_states SET state='healthy', isolated_until=NULL, "
                    "probe_expires_at=NULL, updated_at=? "
                    "WHERE service=? AND revision=? AND node=?",
                    (now, row["service"], row["revision"], row["node"]),
                )
            else:
                # 失败（含试探失败）：隔离 10 秒，试探中的节点继续隔离
                conn.execute(
                    "UPDATE node_states SET state='isolated', isolated_until=?, "
                    "probe_expires_at=NULL, updated_at=? "
                    "WHERE service=? AND revision=? AND node=?",
                    (now + ISOLATION_SECONDS, now,
                     row["service"], row["revision"], row["node"]),
                )

        conn.execute(
            "UPDATE pick_tokens SET reported=1, outcome=?, reported_at=? WHERE token=?",
            (outcome, now, token),
        )
        conn.commit()
        state = _current_node_state(conn, row["service"], row["revision"], row["node"])
        return {
            "applied": applied,
            "reason": reason,
            "service": row["service"],
            "revision": row["revision"],
            "node": row["node"],
            "outcome": outcome,
            "state": state,
        }
    finally:
        conn.close()


def _current_node_state(
    conn: sqlite3.Connection, service: str, revision: int, node: str
) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT * FROM node_states WHERE service=? AND revision=? AND node=?",
        (service, revision, node),
    ).fetchone()
    if r is None:
        return None
    return {
        "node": r["node"],
        "state": r["state"],
        "isolated_until": r["isolated_until"],
        "probe_expires_at": r["probe_expires_at"],
        "probe_epoch": r["probe_epoch"],
    }


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(
    title=f"路由查询缓存 API（{INSTANCE_ID}）",
    description=(
        "两个缓存实例共享 SQLite 状态，提供跨实例并发合并、租约接管与"
        "修订一致性保证。/docs 可直接操作查询、快照排障与状态检查。"
    ),
    version="1.0.0",
)


@app.on_event("startup")
async def _startup() -> None:
    init_db()


class QueryResult(BaseModel):
    service: str
    revision: int
    nodes: list[str]
    served_by: str
    from_cache: bool
    coalesced: bool | None = None
    degraded: bool | None = None
    cached_at: float | None = None
    snapshot_status: str | None = None


class PickResult(BaseModel):
    service: str
    revision: int
    node: str
    token: str
    kind: str  # normal | probe
    probe: bool
    isolation_seconds: float
    served_by: str
    issued_at: float
    expires_at: float


class ReportRequest(BaseModel):
    token: str = Field(..., description="选点 API 返回的一次性回报凭据")
    outcome: str = Field(
        ..., description="连接结果：success=连接成功；failure=连接失败"
    )


class ReportResult(BaseModel):
    applied: bool
    reason: str  # applied | duplicate | stale | stale_revision
    service: str
    revision: int
    node: str
    outcome: str
    state: dict[str, Any] | None = None


@app.get("/routes/{service}", response_model=QueryResult, summary="查询服务当前路由（含修订）")
async def query_route(service: str) -> dict[str, Any]:
    return await _query(service)


@app.post(
    "/pick/{service}",
    response_model=PickResult,
    summary="选点：返回一个目标节点与一次性回报凭据（自动隔离故障节点）",
)
async def pick_node(service: str) -> dict[str, Any]:
    return await _pick(service)


@app.post(
    "/reports",
    response_model=ReportResult,
    summary="回报连接成败（凭据一次性；陈旧/重复回报不改写当前状态）",
)
async def report_connection(req: ReportRequest) -> dict[str, Any]:
    if req.outcome not in {"success", "failure"}:
        raise HTTPException(
            status_code=422,
            detail={"error": "outcome must be 'success' or 'failure'"},
        )
    return await _report(req.token, req.outcome)


@app.get("/snapshots/{service}", summary="列出该服务全部缓存快照（含 stale，供排障）")
async def list_snapshots(service: str) -> dict[str, Any]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT service, revision, nodes, status, source_published_at, cached_at "
            "FROM snapshots WHERE service=? ORDER BY revision",
            (service,),
        ).fetchall()
        return {
            "service": service,
            "snapshots": [
                {
                    "revision": r["revision"],
                    "nodes": json.loads(r["nodes"]),
                    "status": r["status"],
                    "source_published_at": r["source_published_at"],
                    "cached_at": r["cached_at"],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/snapshots/{service}/revisions/{revision}", summary="查看指定修订的历史快照（排障）")
async def get_snapshot(service: str, revision: int) -> dict[str, Any]:
    conn = _connect()
    try:
        r = _get_snapshot(conn, service, revision)
        if r is None:
            raise HTTPException(status_code=404, detail="snapshot not found")
        return {
            "service": service,
            "revision": revision,
            "nodes": json.loads(r["nodes"]),
            "status": r["status"],
            "source_published_at": r["source_published_at"],
            "cached_at": r["cached_at"],
        }
    finally:
        conn.close()


@app.get("/debug/state", summary="共享数据库内部状态（inflight/failures/快照/选点隔离）")
async def debug_state() -> dict[str, Any]:
    conn = _connect()
    try:
        inflight = [
            dict(r)
            for r in conn.execute(
                "SELECT service, revision, leader, lease_expires_at, started_at FROM inflight"
            ).fetchall()
        ]
        failures = [dict(r) for r in conn.execute("SELECT * FROM failures").fetchall()]
        snaps = [
            {
                "service": r["service"],
                "revision": r["revision"],
                "status": r["status"],
                "cached_at": r["cached_at"],
            }
            for r in conn.execute(
                "SELECT service, revision, status, cached_at FROM snapshots "
                "ORDER BY service, revision"
            ).fetchall()
        ]
        node_states = [
            dict(r)
            for r in conn.execute(
                "SELECT service, revision, node, state, isolated_until, "
                "probe_expires_at, probe_epoch, updated_at FROM node_states "
                "ORDER BY service, revision, node"
            ).fetchall()
        ]
        tokens = [
            dict(r)
            for r in conn.execute(
                "SELECT token, service, revision, node, kind, probe_epoch, issued_by, "
                "issued_at, expires_at, reported, outcome, reported_at FROM pick_tokens "
                "ORDER BY issued_at DESC LIMIT 100"
            ).fetchall()
        ]
        return {
            "instance_id": INSTANCE_ID,
            "now": time.time(),
            "inflight": inflight,
            "failures": failures,
            "snapshots": snaps,
            "node_states": node_states,
            "pick_tokens": tokens,
        }
    finally:
        conn.close()


@app.post("/debug/reset", summary="清空共享缓存状态（测试用）")
async def debug_reset() -> dict[str, Any]:
    conn = _connect()
    try:
        conn.execute("DELETE FROM snapshots")
        conn.execute("DELETE FROM inflight")
        conn.execute("DELETE FROM failures")
        conn.execute("DELETE FROM node_states")
        conn.execute("DELETE FROM pick_tokens")
        conn.execute("DELETE FROM pick_cursors")
    finally:
        conn.close()
    return {"reset": True, "by": INSTANCE_ID}


@app.get("/health", summary="健康检查")
async def health() -> dict[str, str]:
    return {"status": "ok", "instance_id": INSTANCE_ID}
