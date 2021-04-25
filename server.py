import os
from time import time
from utils import routine
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv
from config import config
from database import match_initial, match_update


env_path = Path("./private/.env")
load_dotenv(dotenv_path=env_path)

if __name__ == "__main__":

    server_start_time = time()
    sheet = config()
    mid = match_initial()

    student_page = sheet.worksheet_by_title("student")
    upload_page = sheet.worksheet_by_title("upload")
    info_page = sheet.worksheet_by_title("information")
    history_page = sheet.worksheet_by_title("history")

    success_num = routine(mid, upload_page, student_page, info_page, history_page, os.getenv("upload_root_path"))
    server_end_time = time()
    server_execute_time = float('{:.2f}'.format((server_end_time - server_start_time) / 60))
    match_update(server_execute_time, success_num)
    logger.info(f"code execute time: {server_execute_time}, agent_num: {success_num}")