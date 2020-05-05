import asyncio

import uvloop

from core.managers import (
    DatabaserManager,
)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

if __name__ == '__main__':
    manager = DatabaserManager()
    manager.manage()
