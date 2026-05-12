from __future__ import annotations

import argparse

import uvicorn


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Capital.com trading bot - web server")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--reload", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    # Server starts even without valid config / credentials.
    uvicorn.run("src.web.server:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
