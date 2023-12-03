import asyncio
import logging
from importlib import import_module

# Project
from config import Database, DeclarativeBase

logger = logging.getLogger(__name__)


async def async_main() -> None:
    db = Database()
    engine = db.engine

    import_module("models")
    async with engine.begin() as conn:
        await conn.run_sync(DeclarativeBase.metadata.create_all)

    await engine.dispose()
    logger.info(f"Done create tables.")


asyncio.run(async_main())
