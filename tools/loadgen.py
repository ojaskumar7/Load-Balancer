from __future__ import annotations

import argparse
import asyncio
from collections import Counter


async def request(
    host: str,
    port: int,
    path: str,
    semaphore: asyncio.Semaphore,
) -> str:
    async with semaphore:
        writer: asyncio.StreamWriter | None = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5
            )
            payload = (
                f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
            ).encode()
            writer.write(payload)
            await writer.drain()
            data = await asyncio.wait_for(reader.read(), timeout=10)
            _, _, body = data.partition(b"\r\n\r\n")
            return body.decode(errors="replace").strip() or "<empty>"
        except (OSError, asyncio.TimeoutError) as exc:
            return f"<error: {type(exc).__name__}>"
        finally:
            if writer is not None:
                writer.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Simple HTTP load generator for distribution checks")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--path", default="/")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    args = parser.parse_args()

    semaphore = asyncio.Semaphore(args.concurrency)
    results = await asyncio.gather(
        *(request(args.host, args.port, args.path, semaphore) for _ in range(args.requests))
    )

    counts = Counter(results)
    for body, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        print(f"{count:6d}  {body}")


if __name__ == "__main__":
    asyncio.run(main())
