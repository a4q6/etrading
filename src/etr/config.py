from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

class Config:
    LOG_DIR = os.getenv("LOG_DIR", "./logs")
    TP_DIR = os.getenv("TP_DIR", "./data/tp")
    HDB_DIR = os.getenv("HDB_DIR", "./data/hdb")
    DISCORD_URL_TEST = os.getenv("DISCORD_URL_TEST")
    DISCORD_URL_ALL = os.getenv("DISCORD_URL_ALL")
    DISCORD_URL_BB = os.getenv("DISCORD_URL_BB")
    DISCORD_URL_CC = os.getenv("DISCORD_URL_CC")

    COINCHECK_API_KEY = os.getenv("COINCHECK_API_KEY")
    COINCHECK_API_SECRET = os.getenv("COINCHECK_API_SECRET")
    BITBANK_API_KEY = os.getenv("BITBANK_API_KEY")
    BITBANK_API_SECRET = os.getenv("BITBANK_API_SECRET")

    JQUANTS_ID = os.getenv("JQUANTS_ID")
    JQUANTS_PW = os.getenv("JQUANTS_PW")
    JQUANTS_DB = os.getenv("JQUANTS_DB")
    JQUANTS_API_KEY = os.getenv("JQUANTS_API_KEY")
