import logging
import sys
from aiohttp import ClientSession
import typing
import json

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


def get_value_dynamic_keys(d, keys):
    for key in keys:
        d = d[key]
    return d

def set_value_dynamic_keys(dic, keys, value, create_missing=True):
    d=dic
    for key in keys[:-1]:
        if key in d:
            d = d[key]
        elif create_missing:
            d = d.setdefault(key,{})
        else:
            return dic
    if keys[-1] in d or create_missing:
        d[keys[-1]] = value
        return dic
