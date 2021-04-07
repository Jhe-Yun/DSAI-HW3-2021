import os
import subprocess
import sqlite3
import pandas as pd
from config import get_time
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv


env_path = Path("./private/.env")
load_dotenv(dotenv_path=env_path)
mid = 0


def db_connect():
    conn = sqlite3.connect(os.getenv("db_url"))
    cur = conn.cursor()

    return conn, cur


def match_initial():
    global mid
    conn, cur = db_connect()
    time = get_time()

    cur.execute("INSERT INTO match(time) VALUES (?)", (time,))
    conn.commit()
    test = cur.execute("SELECT * FROM match").fetchall()
    logger.success("insert match info success")

    mid = cur.execute("SELECT mid FROM match WHERE time = ?", (time, )).fetchone()[0]
    logger.info(f"mid: {mid}")

    conn.close()
    return


def bid_insert(student_id, filename, agent, match_time):
    """
    write student bids into db

    parameter:
    - student_id
    - filename
    - agent
    - match_time
    """

    conn, cur = db_connect()
    # cur.execute("")
    os.chdir("./data/output/")
    bids = pd.read_csv(f"./{filename}.csv")
    ### check target_price ###
    ### check student bid time (same date) ###
    bids[["mid", "bidder", "status", "agent"]] = mid, student_id, "已投標", agent
    logger.info(f"{student_id} bids: {bids}")
    try:
        bids.to_sql("bids", con=conn, if_exists="append", index=False)
        logger.success(f"student bids insert into db")
    except Exception as e:
        logger.error(e)

    conn.close()
    logger.info(f"before path= {os.getcwd()}")
    os.chdir(f"../code/{filename}/")
    logger.info(f"after path= {os.getcwd()}")
    return