from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

class Config:
    LOG_DIR = os.getenv("LOG_DIR", "./logs")
    TP_DIR = os.getenv("TP_DIR", "./data/tp")
    HDB_DIR = os.getenv("HDB_DIR", "./data/hdb")
