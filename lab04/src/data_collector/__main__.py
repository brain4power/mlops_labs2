import asyncio
import logging
from sys import stdout
from copy import deepcopy
from datetime import datetime

import requests
from furl import furl
from sqlalchemy import func, insert
from sqlalchemy.ext.asyncio import async_sessionmaker

# Project
from config import Database, settings, DeclarativeBase
from models import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logFormatter = logging.Formatter("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


async def async_main() -> None:
    start_time = datetime.now()

    db = Database()
    engine = db.engine
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    base_api_url = furl(settings.COLLECTOR_BASE_API_URL)

    async with async_session() as session:
        db_min_page_number = await session.execute(func.min(RawData.page_number))
        db_min_page_number = db_min_page_number.scalar() or 700000
        logger.info(f"db_min_page_number: {db_min_page_number}")
        start_page_number = db_min_page_number - 1
        error_count = settings.COLLECTOR_ERROR_COUNT

        while start_page_number > 0 and error_count > 0:
            api_url = deepcopy(base_api_url)
            api_url.path.add(str(start_page_number))
            logger.info(f"Ready to get request to url: {api_url.url}")
            response = requests.get(api_url.url, params={"token": settings.COLLECTOR_ACCESS_TOKEN})
            if response.status_code != 200:
                logger.info(f"Got {response.status_code} status_code. Will take a nap")
                error_count -= 1
                await asyncio.sleep(settings.COLLECTOR_TIMEOUT)
                continue
            data = [
                dict(
                    page_number=start_page_number,
                    deactivated=el.get("deactivated"),
                    country_id=el.get("country").get("id"),
                    country_title=el.get("country").get("title"),
                    city_id=el.get("city").get("id"),
                    city_title=el.get("city").get("title"),
                    about=el.get("about"),
                    activities=el.get("activities"),
                    books=el.get("books"),
                    games=el.get("games"),
                    interests=el.get("interests"),
                    education_form=el.get("education_form"),
                    education_status=el.get("education_status"),
                    university_id=el.get("university"),
                    university_name=el.get("university_name"),
                    faculty_id=el.get("faculty"),
                    faculty_name=el.get("faculty_name"),
                    graduation_year=el.get("graduation"),
                )
                for el in response.json()["response"]
            ]

            await session.execute(insert(RawData), data)
            await session.commit()
            logger.info(f"Done save data for page: {start_page_number}")
            start_page_number -= 1
            logger.info("Will take a nap...")
            await asyncio.sleep(settings.COLLECTOR_TIMEOUT)
            logger.info("Continue collect data")
            logger.info(f"Already running {datetime.now() - start_time}")
    await engine.dispose()
    logger.info(f"Done collect data.")


if __name__ == "__main__":
    asyncio.run(async_main())
