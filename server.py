"""Live price streamer for Open Positions + pivot_watch.

- Subscribes to EODHD's real-time US WebSocket for the union of currently-held
  tickers (latest positions_archive snapshot) and pivot_watch tickers.
- Keeps the latest trade price per symbol in memory.
- Serves an SSE stream (/stream) + JSON snapshot (/prices) on 127.0.0.1, which
  nginx fronts (gated) at /live/. The browser recomputes P&L from static
  entry/qty against the live price and flashes the cells.

No DB writes, no gateway dependency. Read-only DB (claude_ro) only to learn
which symbols to watch.
"""
import asyncio, json, os, time, contextlib
import psycopg
import websockets
from aiohttp import web

EODHD_TOKEN = os.environ["EODHD_TOKEN"]
DB_DSN      = os.environ["DB_DSN"]
WS_URL      = "wss://ws.eodhistoricaldata.com/ws/us?api_token=" + EODHD_TOKEN
# Forex stream for the account currency: GBPUSD mid (bid/ask average), 24x5. Ticks arrive
# several times a second, so broadcasts are throttled to one per FX_MIN_INTERVAL.
FX_WS_URL   = "wss://ws.eodhistoricaldata.com/ws/forex?api_token=" + EODHD_TOKEN
FX_SYMBOLS  = [s.strip().upper() for s in os.environ.get("FX_SYMBOLS", "GBPUSD").split(",") if s.strip()]
FX_MIN_INTERVAL = float(os.environ.get("FX_MIN_INTERVAL_SEC", "1.0"))
PORT        = int(os.environ.get("STREAMER_PORT", "9100"))
SYM_REFRESH = int(os.environ.get("SYM_REFRESH_SEC", "60"))

PRICES: dict[str, dict] = {}      # sym -> {"p": price, "t": epoch_ms}
SUBS: set[str] = set()            # currently-subscribed symbols
CLIENTS: set[asyncio.Queue] = set()
LAST_MSG = {"t": 0.0}
_ws_send = {"fn": None}           # set to the live socket's send coroutine


def wanted_symbols() -> set[str]:
    # open trades in the journal + names on the pivot watch
    q = ("""select ticker from trade_log where lower(status) = 'open'
            union
            select ticker from pivot_watch""")
    try:
        with psycopg.connect(DB_DSN, connect_timeout=8) as c, c.cursor() as cur:
            cur.execute(q)
            return {r[0].strip().upper() for r in cur.fetchall() if r[0]}
    except Exception as e:
        print("symbol query failed:", repr(e)[:120], flush=True)
        return set(SUBS)             # keep the current set on a transient DB error


def broadcast(evt: dict):
    dead = []
    for q in CLIENTS:
        try:
            q.put_nowait(evt)
        except Exception:
            dead.append(q)
    for q in dead:
        CLIENTS.discard(q)


async def ws_loop():
    backoff = 1
    while True:
        try:
            async with websockets.connect(WS_URL, open_timeout=15, ping_interval=20) as ws:
                _ws_send["fn"] = ws.send
                backoff = 1
                SUBS.clear()
                syms = wanted_symbols()
                if syms:
                    await ws.send(json.dumps({"action": "subscribe", "symbols": ",".join(sorted(syms))}))
                    SUBS.update(syms)
                    print("subscribed:", ",".join(sorted(syms)), flush=True)
                async for raw in ws:
                    LAST_MSG["t"] = time.time()
                    try:
                        m = json.loads(raw)
                    except Exception:
                        continue
                    sym = m.get("s")
                    p = m.get("p")
                    if sym and p is not None:
                        try:
                            price = float(p)
                        except (TypeError, ValueError):
                            continue
                        ts = int(m.get("t") or time.time() * 1000)
                        PRICES[sym.upper()] = {"p": price, "t": ts}
                        broadcast({"ticker": sym.upper(), "price": price, "t": ts})
        except Exception as e:
            _ws_send["fn"] = None
            print("ws error, reconnecting:", repr(e)[:140], flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def fx_loop():
    """GBPUSD (and any FX_SYMBOLS) from the forex stream. Stored in PRICES under the pair name
    and broadcast like a ticker, throttled; the page converts USD P&L/value with it."""
    backoff = 1
    last_sent: dict[str, float] = {}
    while True:
        try:
            async with websockets.connect(FX_WS_URL, open_timeout=15, ping_interval=20) as ws:
                backoff = 1
                await ws.send(json.dumps({"action": "subscribe", "symbols": ",".join(FX_SYMBOLS)}))
                print("fx subscribed:", ",".join(FX_SYMBOLS), flush=True)
                async for raw in ws:
                    try:
                        m = json.loads(raw)
                    except Exception:
                        continue
                    sym = (m.get("s") or "").upper()
                    if sym not in FX_SYMBOLS:
                        continue
                    a, b = m.get("a"), m.get("b")
                    try:
                        mid = (float(a) + float(b)) / 2 if (a is not None and b is not None) else float(m.get("p"))
                    except (TypeError, ValueError):
                        continue
                    if mid <= 0:
                        continue
                    ts = int(m.get("t") or time.time() * 1000)
                    PRICES[sym] = {"p": round(mid, 6), "t": ts, "fx": True}
                    now = time.time()
                    if now - last_sent.get(sym, 0) >= FX_MIN_INTERVAL:
                        last_sent[sym] = now
                        broadcast({"ticker": sym, "price": round(mid, 6), "t": ts, "fx": True})
        except Exception as e:
            print("fx ws error, reconnecting:", repr(e)[:140], flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def sym_loop():
    """Keep the subscription in sync with held ∪ pivot_watch as they change."""
    while True:
        await asyncio.sleep(SYM_REFRESH)
        send = _ws_send["fn"]
        if not send:
            continue
        want = wanted_symbols()
        if not want:
            continue
        add = want - SUBS
        rem = SUBS - want
        try:
            if add:
                await send(json.dumps({"action": "subscribe", "symbols": ",".join(sorted(add))}))
                SUBS.update(add); print("subscribe +", ",".join(sorted(add)), flush=True)
            if rem:
                await send(json.dumps({"action": "unsubscribe", "symbols": ",".join(sorted(rem))}))
                SUBS.difference_update(rem)
                for s in rem:
                    PRICES.pop(s, None)
                print("unsubscribe -", ",".join(sorted(rem)), flush=True)
        except Exception as e:
            print("resubscribe failed:", repr(e)[:120], flush=True)


async def handle_prices(request):
    return web.json_response({"prices": PRICES, "subscribed": sorted(SUBS),
                              "last_msg_age_s": round(time.time() - LAST_MSG["t"], 1) if LAST_MSG["t"] else None})


async def handle_health(request):
    return web.json_response({"ok": True, "subscribed": sorted(SUBS),
                              "have_prices": len(PRICES),
                              "fx": {s: PRICES.get(s) for s in FX_SYMBOLS},
                              "ws_up": _ws_send["fn"] is not None})


async def handle_stream(request):
    resp = web.StreamResponse(status=200, headers={
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    })
    await resp.prepare(request)
    q: asyncio.Queue = asyncio.Queue(maxsize=1000)
    CLIENTS.add(q)
    try:
        # initial snapshot so the page paints immediately
        await resp.write(f"event: snapshot\ndata: {json.dumps(PRICES)}\n\n".encode())
        while True:
            try:
                evt = await asyncio.wait_for(q.get(), timeout=15)
                await resp.write(f"data: {json.dumps(evt)}\n\n".encode())
            except asyncio.TimeoutError:
                await resp.write(b": ping\n\n")   # keep-alive comment
    except (asyncio.CancelledError, ConnectionResetError):
        pass
    finally:
        CLIENTS.discard(q)
    return resp


async def main():
    app = web.Application()
    app.router.add_get("/prices", handle_prices)
    app.router.add_get("/stream", handle_stream)
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", PORT)
    await site.start()
    print(f"price-streamer on 127.0.0.1:{PORT}", flush=True)
    await asyncio.gather(ws_loop(), sym_loop(), fx_loop())


if __name__ == "__main__":
    asyncio.run(main())
