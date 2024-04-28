
"""take a snapshot of all validators"""

import asyncio
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse

import aiofiles
import aiohttp
from aiohttp import ClientSession

import helpers

"""logging config"""

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)


"""CONSTANTS"""
#pagination limit is set to 1000, this will include all validators on first page so no need to iterate.
# May need to be updated in the  with proper pagination handling.
VALIDATOR_PAGINATION_LIMIT=1000
#the two below combine in the format API_base_url{chain_name}validator_list_endpoint to query the validator list
API_BASE_URL= "https://rest.cosmos.directory/"
VALIDATOR_LIST_ENDPOINT= "/cosmos/staking/v1beta1/validators"


async def fetch_validator_response(session : ClientSession, chain_name : str, validator_status: str = "BOND_STATUS_BONDED") -> dict:
    full_url = f"{API_BASE_URL}{chain_name}{VALIDATOR_LIST_ENDPOINT}"
    resp_json=dict()
    try:
        resp_json = await helpers.fetch_rest_api(
            url=full_url,
            session=session,
            params={
                    "status": validator_status,
                    "pagination.limit": VALIDATOR_PAGINATION_LIMIT
                })
    except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            chain_name,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
    return resp_json

async def parse_validator_response(resp_json:dict):
    validators=resp_json.get("validators",None)
    print(validators)
    return validators

async def fetch_multiple_validator_types(session: ClientSession, chain_name : str, validator_statuses : list):
    for validator_status in validator_statuses:
       validators = await parse_validator_response(fetch_validator_response(session=session,chain_name=chain_name,validator_status=validator_status))
    print(type(validators))



async def main(chain_name:str):
    #create the AIOHTTP session
    async with ClientSession() as session:
        tasks=[]
        i=0
        while i<1000:
            tasks.append(fetch_validator_response(session=session,chain_name=chain_name))
            i=i+1
        await asyncio.gather(*tasks)

asyncio.get_event_loop().run_until_complete(main("cosmoshub"))