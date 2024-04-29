"""take a snapshot of all validators"""
from __future__ import annotations

import asyncio
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse
import time
import aiofiles
import aiohttp
from aiohttp import ClientSession
import tenacity


import helpers
import json

"""logging config"""

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

"""CONSTANTS"""
# pagination limit is set to 1000, this will include all validators on first page so no need to iterate.
# May need to be updated in the  with proper pagination handling.
VALIDATOR_PAGINATION_LIMIT = 1000
# the two below combine in the format API_base_url{chain_name}validator_list_endpoint to query the validator list
API_BASE_URL = "https://rest.cosmos.directory/"
VALIDATOR_LIST_ENDPOINT = "/cosmos/staking/v1beta1/validators"
CHAINS_API_URL = "https://chains.cosmos.directory/"



@tenacity.retry(stop=tenacity.stop_after_attempt(2), wait=tenacity.wait_fixed(0.5),
                after=tenacity.after_log(logging, logging.DEBUG))
async def fetch_validator_response(session: ClientSession, chain_name: str,
                                   validator_status: str = "BOND_STATUS_BONDED") -> dict:
    """
    :param ClientSession session:
    :param str chain_name:
    :param str validator_status:
    :return dict:
    """
    full_url = f"{API_BASE_URL}{chain_name}{VALIDATOR_LIST_ENDPOINT}"
    resp_json = dict()
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
        raise IOError
    return resp_json


async def parse_validator_response(resp_json: dict) -> list:
    validators = resp_json.get("validators", None)
    return validators



async def fetch_and_parse_validator_snapshot(session: ClientSession, chain_name: str, validator_status: str):
    try:
        validators = await parse_validator_response(
            await fetch_validator_response(
                session=session,
                chain_name=chain_name,
                validator_status=validator_status
            )
        )
        logger.info("Parsed %d validators for %s of status %s", len(validators), chain_name, validator_status)
        chain_validators={"chain":chain_name, "status": validator_status, "time" : str(time.time()), "validator_response" : validators}
        return chain_validators
    except Exception as e:
        chain_validators = {"chain": chain_name, "status": validator_status, "time" : str(time.time()),"validator_response": []}
        return chain_validators
        logger.error(
            "parsing exception for %s on %s [%s]: %s",
            chain_name,
            validator_status,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )



async def get_chain_list(session : ClientSession):
    chainlist = []

    # Send a GET request to the chains list page
    response = await helpers.fetch_rest_api(CHAINS_API_URL,session=session)

    for chain in response["chains"]:
        # Get the chain ID and URL from the chain object
        chain_id = chain["name"]
        chainlist.append(chain_id)
    return chainlist


async def get_all_chains_validators(validator_statuses:[str] = ["BOND_STATUS_BONDED"]):
    # create the AIOHTTP session
    async with ClientSession() as session:
        chain_list= await get_chain_list(session)
        tasks = []
        for chain_name in chain_list:
            for status in validator_statuses:
                # this is much faster since it goes async
                tasks.append(fetch_and_parse_validator_snapshot(session=session, chain_name=chain_name,
                                                                validator_status=status))

        # this returns the results of all the tasks in a list
        all_chains_results = await asyncio.gather(*tasks)
    return all_chains_results

##todo take filepath, take validator statuses, take optional dump, return validator results
def main():
    start_time = time.time()
    loop=asyncio.get_event_loop()
    results = loop.run_until_complete(get_all_chains_validators(validator_statuses=["BOND_STATUS_BONDED"]))
    with open("validator_snapshot.json", "w") as file:
        json.dump(results, file)
    end_time = time.time()
    runtime = end_time - start_time
    print(runtime)

if __name__ == "__main__":
    main()