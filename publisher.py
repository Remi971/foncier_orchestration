import redis
import json
from dependencies import env

class Publisher:
    def __init__(self):
        self.redis_client = redis.Redis(host=env.REDIS_HOST, port=env.REDIS_PORT, db=env.REDIS_DB)
        
    def subscribe(self, channel: str):
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(channel)
        return pubsub

    def publish_event(self, channel: str, event: dict) -> None:
        try:
            self.redis_client.publish(channel, json.dumps(event))
        except Exception as error:
            print(f"Error publishing event to channel {channel} : {error}")
            self.redis_client.publish(channel, json.dumps({"type": "ERROR", "message": f"Error publishing event: {error}"}))


