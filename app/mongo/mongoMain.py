import asyncio
from dataclasses import field

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pydantic.dataclasses import dataclass
from src import CharvakaFunction

from app.mongo.ConnectToMongoDB import ConnectToMongoDB

CharvakaFunction

@dataclass
class User:
    name:str
    _password:str = field(repr=False)

async def main():
    connectToMongo = ConnectToMongoDB()
    res = await connectToMongo.getDataBaseNames()
    print(res)

if __name__ == "__main__":
    # asyncio.run(main())
    user = User("shyam","shyam@123")
    print(user)