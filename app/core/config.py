import os
from typing import Optional
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

class GlobalConfig(BaseSettings):
    DATABASE_URL: Optional[str] = None
    DB_FORCE_ROLLBACK: bool = False


class DevConfig(GlobalConfig):
    model_config = SettingsConfigDict(env_file = ".env", env_prefix = "DEV_", extra = "ignore")


class ProdConfig(GlobalConfig):
    model_config = SettingsConfigDict(env_file = ".env", env_prefix = "PROD_", extra = "ignore")


@lru_cache()
def get_config(env_state: str):
    configs = {
        "dev": DevConfig,
        "prod": ProdConfig
    }
    return configs[env_state]()


# Read ENV_STATE directly from environment or .env file

load_dotenv()

config = get_config(os.getenv("ENV_STATE", "dev"))

pass
