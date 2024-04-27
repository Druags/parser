import os

from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = ROOT_PATH + '\data\\'


class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASS: str
    DB_NAME: str

    @property
    def DATABASE_URL_mysql(self):
        return f"mysql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    model_config = SettingsConfigDict(env_file=ROOT_PATH + '\.env')
