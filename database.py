import os
from motor.motor_asyncio import AsyncIOMotorClient

# MongoDB URL 
MONGO_URI = os.getenv("MONGO_URI", "YOUR_MONGODB_URL_HERE")
client = AsyncIOMotorClient(MONGO_URI)
db = client['MegaRenamerBot']
users_collection = db['users']

async def add_user(user_id: int):
    user = await users_collection.find_one({"_id": user_id})
    if not user:
        new_user = {
            "_id": user_id,
            "lifetime_renamed": 0,
            "daily_limit": 100,
            "is_premium": False
        }
        await users_collection.insert_one(new_user)
        return True
    return False

async def get_user(user_id: int):
    return await users_collection.find_one({"_id": user_id})

async def update_rename_stats(user_id: int, files_renamed: int):
    await users_collection.update_one(
        {"_id": user_id},
        {
            "$inc": {"lifetime_renamed": files_renamed, "daily_limit": -files_renamed}
        }
    )
