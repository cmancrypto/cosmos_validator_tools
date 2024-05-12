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

    # Create the database object
    return client['validatorTools']

def get_collection():
    db=get_database()
    collection=db["validatorSnapshots"]
    return collection



