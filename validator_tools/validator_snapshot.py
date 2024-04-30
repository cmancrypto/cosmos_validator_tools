"""take a snapshot of all validators on all chains matching the desired BOND_STATUS type"""

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
        chain_validators={"chain":chain_name, "status": validator_status, "time" : time.strftime("%Y-%m-%d", time.gmtime()), "validator_response" : validators}
        return chain_validators
    except Exception as e:
        chain_validators = {"chain": chain_name, "status": validator_status, "time" : time.strftime("%Y-%m-%d", time.gmtime()),"validator_response": []}
        logger.error(
            "parsing exception for %s on %s [%s]: %s",
            chain_name,
            validator_status,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )

        return chain_validators



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

class DumpJson:
    """
    dump : bool
    filepath : str - required if dump = True
    """
    def __init__(self, dump: bool = False, filepath: str = None):
        self.dump = dump
        if self.dump == True and filepath == None:
            raise ValueError("need file path if DumpJson dump True")
        self.filepath = filepath


##todo take filepath, take validator statuses, take optional dump, return validator results
def main(
        dump_json: DumpJson,
         bond_status_list: list = ["BOND_STATUS_BONDED"],
         validator_results_filters: list=[]
         )-> list:
    """
    :param dump_json: DumpJson - validator_snapshot.DumpJson type - a neat wrapper to allow bool to determine dumping to json and providing a filepath for outputting JSON
    :param bond_status_list: list - a list of all of the bond status' to be queried
    :return: list -> this returns a list of dicts for each chain and bond status format is
    """
    start_time = time.time()
    loop=asyncio.get_event_loop()
    all_chain_results = loop.run_until_complete(get_all_chains_validators(validator_statuses=bond_status_list))

    ##if filters are specified, filter the individual validator response
    #todo, allow for multiple deep nested response using helpers for dynamic nested
    if len(validator_results_filters) > 0:
        filtered_results=[]
        #print(all_chain_results)
        for i,chain_result in enumerate(all_chain_results):
            all_validator_data=[]
            if len(chain_result[i]["validator_response"])>0:
                for j,validator_results in enumerate(chain_result["validator_response"]):
                    validator_data = {}
                    for filter in validator_results_filters:
                        try:
                            validator_data[filter]=all_chain_results[i]["validator_response"][j][filter]
                        except KeyError as e:
                            logger.error(
                                "key error  for %s on status %s of [%s]: %s",
                                chain_result[i]["chain"],
                                chain_result[i]["status"],
                                getattr(e, "status", None),
                                getattr(e, "message", None),
                            )
                    all_validator_data.append(validator_data)
                all_chain_results[i]["validator_response"]=all_validator_data
            else:
                logger.info("No validators or result error recorded for %s",all_chain_results[i]["chain"])
        print(all_chain_results)



    if dump_json.dump == True:
        with open(dump_json.filepath, "w") as file:
            json.dump(all_chain_results, file)
    end_time = time.time()
    runtime = end_time - start_time
    logger.info("Total runtime was %s seconds", runtime)

    return all_chain_results



if __name__ == "__main__":
    results=main(
        DumpJson(dump=True,filepath="validator_snapshot.json"),
        bond_status_list=["BOND_STATUS_BONDED"],
        validator_results_filters=["operator_address","tokens","moniker"]
    )
    #print(results)