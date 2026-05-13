from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from urllib.parse import parse_qs, quote

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from src.utils.logger import setup_logging
from src.web.state import AppState, load_state, try_login, try_logout

TEMPLATES_DIR = Path(__file__).parent / "templates"

ACCESS_PASSWORD = "Pietro73"
ACCESS_COOKIE = "tcdc_auth"
ACCESS_TOKEN = "ok"
PUBLIC_PATHS = {"/login", "/healthz"}


def _is_authed(request: Request) -> bool:
    return request.cookies.get(ACCESS_COOKIE) == ACCESS_TOKEN


def create_app(config_path: str = "config.yaml") -> FastAPI:
    setup_logging()
    load_dotenv()

    state: AppState = load_state(config_path)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Best-effort login on startup; never blocks server start.
        await try_login(state)
        scheduler_task = None
        if state.engine is not None:
            import asyncio
            scheduler_task = asyncio.create_task(state.engine.run_scheduler())
        yield
        if scheduler_task is not None and not scheduler_task.done():
            scheduler_task.cancel()
            try:
                await scheduler_task
            except Exception:
                pass
        if state.client is not None:
            await state.client.aclose()

    app = FastAPI(title="Capital.com Trading Bot", version="0.1.0", lifespan=lifespan)
    app.state.bot = state

    @app.middleware("http")
    async def auth_gate(request: Request, call_next):
        path = request.url.path
        if path in PUBLIC_PATHS or _is_authed(request):
            return await call_next(request)
        if path.startswith("/api/"):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return RedirectResponse(url="/login", status_code=303)

    @app.get("/login", response_class=HTMLResponse)
    async def login_form(request: Request, error: str | None = None):
        err_html = (
            f'<p style="color:#c0392b;margin:0 0 12px 0;">{error}</p>' if error else ""
        )
        return HTMLResponse(
            f"""<!doctype html><html><head><meta charset="utf-8"><title>Login</title>
<style>body{{font-family:system-ui,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#f4f6f8;}}
.card{{background:#fff;padding:24px 28px;border-radius:8px;box-shadow:0 2px 12px rgba(0,0,0,.08);min-width:280px;}}
h1{{font-size:18px;margin:0 0 16px 0;}}
input[type=password]{{width:100%;padding:8px 10px;border:1px solid #ccc;border-radius:4px;box-sizing:border-box;margin-bottom:12px;}}
button{{background:#2c7be5;color:#fff;border:0;padding:8px 14px;border-radius:4px;cursor:pointer;width:100%;}}</style>
</head><body><form class="card" method="post" action="/login">
<h1>Inserisci password</h1>{err_html}
<input type="password" name="password" autofocus required />
<button type="submit">Accedi</button></form></body></html>"""
        )

    @app.post("/login")
    async def login_submit(request: Request):
        body = (await request.body()).decode("utf-8", errors="replace")
        fields = parse_qs(body)
        password = (fields.get("password") or [""])[0]
        if password == ACCESS_PASSWORD:
            resp = RedirectResponse(url="/", status_code=303)
            resp.set_cookie(
                ACCESS_COOKIE, ACCESS_TOKEN, httponly=True, samesite="lax", max_age=60 * 60 * 24 * 30
            )
            return resp
        return RedirectResponse(url=f"/login?error={quote('Password errata')}", status_code=303)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "uptime_s": (datetime.now(timezone.utc) - state.started_at).total_seconds()}

    @app.get("/api/status")
    async def status():
        plan = []
        manual_active: list[dict] = []
        if state.engine is not None:
            try:
                plan = [
                    {
                        "strategy": s.strategy_name,
                        "symbol": s.symbol,
                        "session": s.session_id,
                        "start_utc": s.start_utc.isoformat(),
                        "end_utc": s.end_utc.isoformat(),
                    }
                    for s in state.engine.plan_today()
                ]
                manual_active = state.engine.manual_status_snapshot()
            except Exception as e:
                plan = [{"error": str(e)}]
        return JSONResponse({
            "ready": state.ready,
            "server_time_utc": datetime.now(timezone.utc).isoformat(),
            "account_mode": state.config.account.mode if state.config else None,
            "config_error": state.config_error,
            "credentials_ok": state.credentials_ok,
            "credentials_error": state.credentials_error,
            "logged_in": state.logged_in,
            "last_login_error": state.last_login_error,
            "instruments": [m.symbol for m in state.config.instruments] if state.config else [],
            "sessions": list(state.config.sessions.keys()) if state.config else [],
            "strategies": [
                {"name": s.name, "enabled": s.enabled,
                 "deployments": [{"symbol": d.symbol, "sessions": d.sessions} for d in s.deployments]}
                for s in (state.config.strategies if state.config else [])
            ],
            "plan_today": plan,
            "manual_active": manual_active,
            "session_summaries": (state.engine.session_summaries[-50:] if state.engine else []),
            "debug_mode": (state.engine.debug_mode if state.engine else False),
        })

    @app.post("/api/login")
    async def relogin():
        await try_login(state)
        return {"logged_in": state.logged_in, "error": state.last_login_error}

    @app.post("/api/logout")
    async def logout():
        await try_logout(state)
        return {"logged_in": state.logged_in}

    @app.post("/api/manual-start")
    async def manual_start(payload: dict):
        if state.engine is None:
            return JSONResponse({"ok": False, "error": "not logged in"}, status_code=400)
        strategy = payload.get("strategy")
        symbol = payload.get("symbol")
        session = payload.get("session")
        if not strategy or not symbol:
            return JSONResponse(
                {"ok": False, "error": "missing 'strategy' or 'symbol'"}, status_code=400
            )
        try:
            sched = state.engine.trigger_manual(strategy, symbol, session)
        except ValueError as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
        # Bootstrap (ATR + on_session_start) and launch the polling loop so
        # 5m/15m candles are fetched and routed to the strategy.
        try:
            await state.engine.start_polled_instance(sched)
        except Exception as e:
            return JSONResponse(
                {"ok": False, "error": f"bootstrap failed: {e}"}, status_code=500
            )
        return {
            "ok": True,
            "strategy": sched.strategy_name,
            "symbol": sched.symbol,
            "session": sched.session_id,
            "start_utc": sched.start_utc.isoformat(),
            "end_utc": sched.end_utc.isoformat(),
        }

    @app.post("/api/manual-stop")
    async def manual_stop(payload: dict):
        if state.engine is None:
            return JSONResponse({"ok": False, "error": "engine not running"}, status_code=400)
        instance_id = payload.get("instance_id")
        if not instance_id:
            return JSONResponse({"ok": False, "error": "missing 'instance_id'"}, status_code=400)
        try:
            ok = await state.engine.close_manual(instance_id)
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
        if not ok:
            return JSONResponse({"ok": False, "error": "instance not found"}, status_code=404)
        return {"ok": True, "instance_id": instance_id}

    @app.post("/api/debug-mode")
    async def set_debug_mode(payload: dict):
        if state.engine is None:
            return JSONResponse({"ok": False, "error": "engine not running"}, status_code=400)
        enabled = bool(payload.get("enabled", False))
        state.engine.debug_mode = enabled
        from src.utils.events import EVENTS
        EVENTS.warn("system", f"Debug mode {'ENABLED' if enabled else 'disabled'}",
                    box_minutes=2 if enabled else 15,
                    entry_minutes=1 if enabled else 5)
        return {"ok": True, "debug_mode": enabled}

    @app.get("/api/events")
    async def events(since: str | None = None, limit: int = 200):
        from src.utils.events import EVENTS
        return {"events": EVENTS.snapshot(since_ts=since, limit=limit)}

    @app.post("/api/events/clear")
    async def events_clear():
        from src.utils.events import EVENTS
        n = EVENTS.clear()
        return {"ok": True, "cleared": n}

    @app.post("/api/session-summaries/delete")
    async def session_summaries_delete(payload: dict):
        if state.engine is None:
            return JSONResponse({"ok": False, "error": "engine not running"}, status_code=400)
        instance_id = payload.get("instance_id")
        end_utc = payload.get("end_utc")
        if not instance_id or not end_utc:
            return JSONResponse({"ok": False, "error": "instance_id and end_utc required"}, status_code=400)
        ok = state.engine.delete_session_summary(instance_id, end_utc)
        return {"ok": ok}

    @app.post("/api/session-summaries/clear")
    async def session_summaries_clear():
        if state.engine is None:
            return JSONResponse({"ok": False, "error": "engine not running"}, status_code=400)
        n = state.engine.clear_session_summaries()
        return {"ok": True, "cleared": n}

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        # Pre-compute today's session start in UTC for each session, so the
        # template can render both the instrument-local time and the user's local
        # time (the latter is formatted client-side using the browser tz).
        from datetime import date
        from src.engine.session_clock import session_start_utc
        session_views = []
        if state.config is not None:
            today = date.today()
            for sid, sess in state.config.sessions.items():
                try:
                    start_utc = session_start_utc(sess, today).isoformat()
                except Exception:
                    start_utc = None
                session_views.append({
                    "id": sid,
                    "start_local": sess.start.strftime("%H:%M"),
                    "timezone": sess.timezone,
                    "days": list(sess.days),
                    "start_utc_today": start_utc,
                })
        return templates.TemplateResponse(
            request, "dashboard.html",
            {"s": state, "session_views": session_views},
        )

    return app


app = create_app()
