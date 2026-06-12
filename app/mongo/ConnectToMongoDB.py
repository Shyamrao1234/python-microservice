import json
import time
from datetime import datetime
from unittest import result

from bson import ObjectId
from pygments.lexers import clean
from pymongo import MongoClient
import motor.motor_asyncio

class ConnectToMongoDB:

    def __init__(self):
        uri = "mongodb://dbadmin:d!ff!cult123@192.168.1.123:27018/"
        # uri = "mongodb://dbadmin:d!ff!cult123@192.168.1.126:27018/"
        self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)

    def convert_to_clean_json(self, value):
        if isinstance(value, ObjectId):
            return str(value)

        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, dict):
            return {k: self.convert_to_clean_json(v) for k, v in value.items()}

        if isinstance(value, list):
            return [self.convert_to_clean_json(v) for v in value]

        return value

    async def getDataBaseNames(self):
        result = []
        try:
            db = self.client.get_database("prod_rainy_reactoreEmployeePortal_BANG")
            my_collection = db.get_collection("FRM_testshyam")
            query = {"enabled": True}
            projection = {"name": 1, "createDate": 1}
            start  =  int(time.time() * 1000)
            data = my_collection.find({})
            print(f"Duration taken by find Query {int(time.time() * 1000) - start}" )
            async for x in data:
                result.append(json.dumps(self.convert_to_clean_json(x)))
            return result
        except Exception as e:
            print(f"Error fetching database names: {e}")
            return []


