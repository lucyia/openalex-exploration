from dotenv import dotenv_values
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

ATLAS_URI = dotenv_values(".env")['ATLAS_URI']


def get_connection(db_name, uri=ATLAS_URI):
    """
    Creates and returns the connection to Mongo DB saved in the env var.

    Parameters
    ----------
    db_name : str
        Name of the database.
    uri : str
        Connection string to MongoDB Atlas.

    Returns
    -------
    dict
        Database and client for the provided service.
    """
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_name]
    return {"db": db, "client": client}
