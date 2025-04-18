from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    LOG_DIR = os.getenv("LOG_DIR", "./logs")
    TP_DIR = os.getenv("TP_DIR", "./data")
