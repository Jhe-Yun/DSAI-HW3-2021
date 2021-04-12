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
    return mid


def student_sync(df):
    conn, cur = db_connect()
    df.drop(["student2"], axis=1, inplace=True)
    df.reset_index(inplace=True)
    df.rename(columns={"student1": "sid", "last time": "last_time"}, inplace=True)
    df.set_index("sid", inplace=True)
    logger.info(f"upload_df: {df}")
    try:
        df.to_sql("student", con=conn, if_exists="append")
        logger.success("student_sync db completed")
        conn.close()
        return 200
    except Exception as e:
        logger.error(e)
        conn.rollback()
        conn.close()
        return 400


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
        logger.success(f"{student_id} bids insert into db")
    except Exception as e:
        logger.error(e)

    conn.close()
    os.chdir(f"../code/{filename}/")
    return
