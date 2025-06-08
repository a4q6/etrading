from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

class Config:
    LOG_DIR = os.getenv("LOG_DIR", "./logs")
    TP_DIR = os.getenv("TP_DIR", "./data/tp")
    HDB_DIR = os.getenv("HDB_DIR", "./data/hdb")
    DISCORD_URL_TEST = os.getenv("DISCORD_URL_TEST")

    COINCHECK_API_KEY = os.getenv("COINCHECK_API_KEY")
    COINCHECK_API_SECRET = os.getenv("COINCHECK_API_SECRET")
