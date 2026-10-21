"""Local RPC for the fixed hourly execution engine and saved user intent.

Importing this module does not read credentials, open a ledger, query an
account, launch an engine or serve a port. main() restores the existing ledger's
user intent; a process restart does not replace a manual pause with permission
to trade. Technical loop faults are handled separately from human controls.
"""
from __future__ import annotations

import asyncio
import json
import os
import secrets
import sys
import threading
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "runtime" / "packages"))

from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from execution.status_view import age_status


STRATEGY = "HourlyTrendRange"
SYMBOLS = {"BTCUSDT", "ETHUSDT"}


def activation_check(root=ROOT, *, pid_exists=None) -> bool:
    """Check this installation's fixed parameters, without any legacy review."""
    root = Path(root)
    try:
        settings = json.loads((root / "user_data" / "hourly-settings.json").read_text(encoding="utf-8"))
        if not isinstance(settings, dict) or settings.get("migrated") is not True or settings.get("strategy") != STRATEGY:
            return False
        return True
    except (OSError, ValueError, KeyError, TypeError, ImportError):
        return False


def _load_api_credentials(root):
    """Read only the two existing API fields, with controller env precedence."""
    fields = ("BINANCE_API_KEY", "BINANCE_API_SECRET")
    values = {key: os.environ.get(key, "") for key in fields}
    if not all(values.values()):
        try:
            lines = (Path(root) / ".env").read_text(encoding="utf-8-sig").splitlines()
        except OSError:
            lines = []
        for line in lines:
            if not line.strip() or line.lstrip().startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if key in fields and not values[key]:
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                    value = value[1:-1]
                values[key] = value
    return values[fields[0]], values[fields[1]]


def build_app(engine, username: str, password: str, *, tick_interval: float = 2.0,
              connection_checker=None) -> FastAPI:
    """Build an inert RPC app; lifecycle ticks existing intent, never start()."""
    if not username or not password:
        raise ValueError("本地RPC认证尚未初始化")
    if tick_interval <= 0:
        raise ValueError("tick_interval必须为正数")
    lock = threading.RLock()
    security = HTTPBasic()
    loop_error = None
    snapshot_lock = threading.Lock()
    published = None
    published_at = None
    publication_error = None
    publication_sequence = 0
    busy_count = 0
    streams = getattr(getattr(engine, 'client', None), 'streams', None)

    async def auth(credentials: HTTPBasicCredentials = Depends(security)):
        user_ok = secrets.compare_digest(credentials.username.encode("utf-8"), username.encode("utf-8"))
        password_ok = secrets.compare_digest(credentials.password.encode("utf-8"), password.encode("utf-8"))
        if not (user_ok and password_ok):
            raise HTTPException(status_code=401, detail="本地RPC认证失败", headers={"WWW-Authenticate": "Basic"})

    def busy(change):
        nonlocal busy_count
        with snapshot_lock:
            busy_count += change

    def publish():
        nonlocal published, published_at, publication_error, publication_sequence
        try:
            result = dict(engine.snapshot())
            if loop_error:
                result['loop_error'] = loop_error
            encoded = json.dumps(jsonable_encoder(result), ensure_ascii=False, allow_nan=False)
            captured = time.monotonic()
            with snapshot_lock:
                published, published_at = encoded, captured
                publication_sequence += 1
                publication_error = None
        except Exception:
            with snapshot_lock:
                publication_error = '状态发布未完成，显示上一次完整快照'

    # Construction is inert: snapshot reads existing state, never starts or ticks.
    with lock:
        publish()

    def tick_once():
        nonlocal loop_error
        busy(1)
        try:
            with lock:
                try:
                    if connection_checker is not None:
                        notice = connection_checker.take_notification()
                        if notice is not None:
                            engine.accept_connection_check(notice)
                    engine.tick()
                    loop_error = None
                except Exception as exc:
                    loop_error = "执行循环出现异常，正在核对恢复；新开仓暂时等待"
                    with suppress(Exception):
                        engine.handle_loop_failure(exc)
                finally:
                    publish()
        finally:
            busy(-1)

    async def read_loop():
        while True:
            await asyncio.to_thread(tick_once)
            await asyncio.sleep(tick_interval)

    @asynccontextmanager
    async def lifespan(_app):
        task = asyncio.create_task(read_loop())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            if connection_checker is not None:
                connection_checker.close()
            # Closing RPC does not place orders, cancel exchange protection,
            # clear ledger state or restart another process.

    app = FastAPI(title="固定小时策略执行引擎", lifespan=lifespan)

    @app.get("/api/status", dependencies=[Depends(auth)])
    async def status():
        with snapshot_lock:
            encoded, captured = published, published_at
            sequence, error, executing = publication_sequence, publication_error, busy_count > 0
        if encoded is None:
            raise HTTPException(503, '执行状态尚未完成首次发布，请稍后核对')
        elapsed = max(0, time.monotonic() - captured)
        result = age_status(json.loads(encoded), elapsed)
        result['snapshot_metadata'] = {'version': 'nonblocking-status-v1', 'sequence': sequence,
            'age_seconds': elapsed, 'execution_busy': executing, 'publication_error': error}
        # This is the thread-safe in-memory stream view, never REST or engine state.
        if streams is not None:
            try:
                result['market_stream'] = streams.snapshot()
            except Exception:
                result['snapshot_metadata']['stream_read_error'] = True
        if connection_checker is not None:
            result['connection_check'] = connection_checker.snapshot()
        return JSONResponse(result)

    @app.post('/api/check-connection', dependencies=[Depends(auth)])
    async def check_connection(payload: dict):
        if payload.get('confirmed') is not True:
            raise HTTPException(422, '请确认发起连接检查')
        if connection_checker is None:
            raise HTTPException(503, '连接检查服务尚未初始化')
        try:
            # No engine lock and no Binance I/O: durable registration only.
            return await asyncio.to_thread(connection_checker.request)
        except Exception:
            raise HTTPException(503, '检查受理状态尚未确认，请读取检查状态；不要重复提交') from None

    def check_context():
        with snapshot_lock:
            value = json.loads(published) if published else {}
        orders = [{key: order.get(key) for key in ('kind', 'symbol', 'client_id', 'order_id', 'identity_unavailable')}
                  for order in value.get('connection_check_unknown_orders', value.get('unknown_orders', []))]
        return {'token': value.get('connection_check_token'),
                'symbols': value.get('symbol_enabled', {}), 'unknown_orders': orders}

    # The real checker may bind this nonblocking published-state supplier.
    if connection_checker is not None:
        connection_checker.context_supplier = check_context

    def selected_symbols(payload: dict, *, required=False):
        if "symbols" not in payload:
            if required:
                raise HTTPException(422, "请提供BTCUSDT和ETHUSDT的完整启用选择")
            return None
        selection = payload["symbols"]
        if not isinstance(selection, dict) or set(selection) != SYMBOLS:
            raise HTTPException(422, "symbols必须完整包含BTCUSDT和ETHUSDT，不接受其它品种")
        if any(type(value) is not bool for value in selection.values()):
            raise HTTPException(422, "每个品种的启用值必须为true或false")
        return dict(selection)

    def mutate(method, payload: dict, **kwargs):
        nonlocal loop_error
        if payload.get("confirmed") is not True:
            raise HTTPException(422, "请通过本地界面确认本次操作")
        busy(1)
        try:
            with lock:
                try:
                    result = method(**kwargs)
                    loop_error = None
                    return result if isinstance(result, dict) else {"ok": True}
                except RuntimeError as exc:
                    raise HTTPException(409, str(exc)) from None
                except ValueError as exc:
                    raise HTTPException(422, str(exc)) from None
                except Exception:
                    raise HTTPException(503, "引擎未确认本次操作，请核对实际状态") from None
                finally:
                    publish()
        finally:
            busy(-1)

    @app.post("/api/start", dependencies=[Depends(auth)])
    def start(payload: dict):
        return mutate(engine.start, payload, symbols=selected_symbols(payload))

    @app.post('/api/strategy-equity', dependencies=[Depends(auth)])
    def strategy_equity(payload: dict):
        return mutate(engine.set_strategy_equity, payload, symbol=payload.get('symbol'),
            target_equity=payload.get('target_equity'), expected_revision=payload.get('expected_revision'),
            operation_id=payload.get('operation_id'))

    @app.get('/api/strategy-equity/{operation_id}', dependencies=[Depends(auth)])
    def equity_result(operation_id: str):
        from execution.equity_adjustments import validate_request
        try:
            validate_request('BTCUSDT', '1', 0, operation_id)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
        return {'ok': True, 'adjustment': engine.store.equity_adjustment(operation_id)}

    @app.post("/api/symbols", dependencies=[Depends(auth)])
    def symbols(payload: dict):
        return mutate(engine.set_symbols, payload, symbols=selected_symbols(payload, required=True))

    @app.post("/api/pause", dependencies=[Depends(auth)])
    def pause(payload: dict):
        return mutate(engine.pause, payload)

    @app.post("/api/exit-all", dependencies=[Depends(auth)])
    def exit_all(payload: dict):
        return mutate(engine.exit_all, payload)

    return app


def main():
    from server.network_environment import apply_network_environment
    apply_network_environment(ROOT, os.environ)
    # The original live database is mandatory; do not silently create a new book
    # or invent a fresh user intention after a process restart.
    ledger = ROOT / "user_data" / "trades-live.sqlite"
    if not ledger.is_file():
        raise RuntimeError("原实盘账本不存在，未创建新账本或启动交易")
    key, secret = _load_api_credentials(ROOT)
    user = os.environ.get("HOURLY_RPC_USER", "")
    password = os.environ.get("HOURLY_RPC_PASSWORD", "")
    if not key or not secret or not user or not password:
        raise RuntimeError("API或本地RPC凭据尚未初始化")
    from execution.binance import BinanceClient
    from execution.engine import Engine, SOFTWARE_RELEASE
    from execution.diagnostics import ExecutionDiagnostics
    from execution.store import Store
    from execution.streams import MarketStreams
    from execution.connection_check import ConnectionCheck
    client = BinanceClient(key, secret)
    diagnostics = ExecutionDiagnostics(ROOT / 'user_data/execution-diagnostics.sqlite', release=SOFTWARE_RELEASE)
    client.diagnostics = diagnostics
    streams = MarketStreams(exchange_clock_ms=client.calibrated_time_bounds_ms)
    client.streams = streams
    engine = Engine(client, Store(ledger), activation_check,
                    restore_saved_intent=True, diagnostics=diagnostics)
    checker = ConnectionCheck(ROOT / 'user_data/connection-check.sqlite',
        lambda: BinanceClient(key, secret, budget=client.budget, timeout=8, get_retries=0), streams)
    checker.restore()
    app = build_app(engine, user, password, connection_checker=checker)
    import uvicorn
    streams.start()
    try:
        uvicorn.run(app, host="127.0.0.1", port=8791, access_log=False, log_level="warning")
    finally:
        streams.stop()
        close_client = getattr(getattr(engine, 'client', None), 'close', None)
        if callable(close_client):
            close_client()


if __name__ == "__main__":
    main()
