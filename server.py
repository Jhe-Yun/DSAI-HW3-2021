import os
from utils import routine
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv
from config import config
from database import match_initial


env_path = Path("./private/.env")
load_dotenv(dotenv_path=env_path)

if __name__ == "__main__":

    sheet = config()
    mid = match_initial()

    student_page = sheet.worksheet_by_title("student")
    upload_page = sheet.worksheet_by_title("upload")
    info_page = sheet.worksheet_by_title("information")

    routine(mid, upload_page, student_page, info_page, os.getenv("upload_root_path"))
    logger.info("routine done")

    #file_manage("P76097612")
    # print(page.get_values("A1", "B100", returnas='range')[0][0])
    # page.update_value("A10", "GG")

    # c1 = page.cell("H1")
    # c1.color = (1, 0, 0, 0)