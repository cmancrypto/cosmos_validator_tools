from pymongo import MongoClient
from pymongo.server_api import ServerApi
import validator_snapshot
from dotenv import load_dotenv
import os

load_dotenv()

def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING, server_api=ServerApi('1'))

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['validatorTools']

def get_collection():
    db=get_database()
    collection=db["validatorSnapshots"]
    return collection



# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":
    # Get the database
    mycollection = get_collection()

    results=validator_snapshot.main(
        validator_snapshot.DumpJson(dump=True,filepath="validator_snapshot.json"),
        bond_status_list=["BOND_STATUS_BONDED"],
        validator_results_filters=[["operator_address"],["tokens"],["status"],["description","moniker"]]##this is a list of lists, where a filter needs to be nested i.e validator_response[i][j] - these should be in format ["i","j"]
    )

    for result in results:
        x=mycollection.insert_one(result)
