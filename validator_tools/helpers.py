import logging
import sys
from aiohttp import ClientSession

##set up logging config to log events to terminal
logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)
async def fetch_rest_api(url: str, session: ClientSession, **kwargs):
    """Async wrapper to request response from Cosmos REST API"""
    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    logger.info("Got response [%s] for URL: %s", resp.status, url)
    resp_json = await resp.json()
    return resp_json