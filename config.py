import os
import time
import pygsheets
from datetime import datetime
from loguru import logger


def config():

    logger.add("./log/{time}.log", rotation="00:00", retention="30 days")
    auth = pygsheets.authorize(service_account_file="./private/credentials.json")
    sheet = auth.open_by_url(os.getenv("sheet_url"))
    logger.info("get auth")

    return sheet


def get_time():
    return time.strftime('%Y-%m-%d %H:00:00', time.localtime())