from __future__ import annotations

import argparse
import asyncio


async def handle(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    delay: float,
) -> None:
    try:
        await asyncio.sleep(delay)
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        writer.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Slow echo backend for load balancer tests")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--ports", type=int, nargs="+", default=[43010])
    parser.add_argument("--delay", type=float, default=2.0)
    args = parser.parse_args()

    servers = [
        await asyncio.start_server(
            lambda reader, writer: handle(reader, writer, args.delay),
            args.host,
            port,
        )
        for port in args.ports
    ]
    print(f"slow echo backends on {args.host} {args.ports} delay={args.delay}s", flush=True)
    await asyncio.gather(*(server.serve_forever() for server in servers))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
