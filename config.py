import os
import time
import pygsheets
import sqlite3
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from loguru import logger


def config():

    logger.add("./log/{time}.log", rotation="00:00", retention="30 days")
    auth = pygsheets.authorize(service_account_file="./private/credentials.json")
    sheet = auth.open_by_url(os.getenv("sheet_url"))
    logger.info("get auth")

    conn = sqlite3.connect(os.getenv("db_url"))
    cur = conn.cursor()

    return sheet, conn, cur


def get_time():
    return time.strftime('%Y-%m-%d %H:00:00', time.localtime())