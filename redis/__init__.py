import redis
import time


from ..logger import CLogger, Color


class Redis:
    """
    Класс для работы с Redis
    """

    def __init__(self, config: dict, attempts: int = 10):
        self.config = config
        self.redis_connect_attempts = attempts
        self.redis_connected = False
        self.redis_client = None

    def __enter__(self):
        return self if self.connect() else None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def redis_info(self) -> str:
        return f"{self.config['Host']}:{self.config['Port']} db:{self.config['DB']}"

    def connect(self):
        if not self.redis_connected:
            self.redis_client = redis.StrictRedis(host=self.config["Host"],
                                                  port=self.config["Port"],
                                                  db=self.config["DB"],
                                                  password=self.config["Password"],
                                                  decode_responses=True)
            for attempt in range(self.redis_connect_attempts):
                try:
                    self.redis_connected = self.redis_client.ping()
                    CLogger.info(f"[CONNECT] Connect to REDIS: {Color.Blue}{self.redis_info()}."
                                 f" {Color.Light_Green}OPENED!")
                    return True
                except Exception as inst:
                    CLogger.exception(exception=inst,
                                      text=f"Can't connect to redis {self.redis_info()}. Ping() fail!")
                    time.sleep(0.1)
            CLogger.error(f"Can't connect to Redis server! {self.redis_info()}")
            return False

    def cleanup(self):
        CLogger.info(f"[CLEANUP] Connect to REDIS: {Color.Blue}{self.redis_info()}. {Color.Light_Red}CLOSED!")
        if self.redis_connected:
            self.redis_client.connection_pool.disconnect()
            self.redis_connected = False

    def reconnect(self):
        self.cleanup()
        self.connect()
