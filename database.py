import os
import subprocess
import sqlite3
import pandas as pd
from config import get_time
from datetime import datetime, timedelta
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


def bids_insert(student_id, filename, flag, agent, match_time):
    """
    write student bids into db

    Parameter:
    - student_id
    - filename
    - flag
    - agent
    - match_time(%Y-%m-%d)
    """

    conn, cur = db_connect()
    filepath = "./data/output/"
    match_time = datetime.strptime(match_time, "%Y%m%d")
    bids = pd.read_csv(f"{filepath}{filename}.csv")
    logger.info(f"before filter {student_id} bids: {bids}")

    # check student output file
    bids["target_price"] = bids["target_price"].map(lambda x: float("{:.2f}".format(x)))
    bids["target_volume"] = bids["target_volume"].map(lambda x: float("{:.2f}".format(x)))
    bids["time"] = bids["time"].map(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    bids = bids[(bids["time"] >= (match_time + timedelta(days=1))) & (bids["time"] < (match_time + timedelta(days=2)))]

    # add column for match
    bids[["mid", "bidder", "status", "flag", "agent"]] = mid, student_id, "已投標", flag, agent
    try:
        if not bids.empty:
            bids.to_sql("bids", con=conn, if_exists="append", index=False)
            logger.success(f"{student_id} bids insert into db")
        else:
            logger.info(f"{student_id} bids no data")
    except Exception as e:
        logger.error(e)

    conn.close()
    return


def bids_get(mid, time, flag):
    """
    for match.py and bill.py

    Parameter:
    - mid
    - time(%Y-%m-%d %H:%M:%S)
    - flag
    """

    conn, cur = db_connect()
    query = f'''SELECT *
                FROM bids
                WHERE mid = {mid} and time = '{time}' and flag = {flag}'''
    data = pd.read_sql(query, conn)

    conn.close()
    return data


def bids_update(buys, sells):
    """
    update bid status after match

    Parameter:
    - buys
    - sells
    """

    conn, cur = db_connect()

    for action in [buys, sells]:
        for bid in action:
            bid_value = float("{:.2f}".format(bid.value))
            bid_price = float("{:.2f}".format(bid.price))

            cur.execute('''SELECT target_volume
                           FROM bids
                           WHERE bid = ?''',
                        (bid.id, ))
            target_volume = cur.fetchone()[0]

            status = ("完全成交"
                      if target_volume == bid.value
                      else "部分成交")

            cur.execute('''UPDATE bids
                           SET closing_price = ?, closing_volume = ?, status = ?
                           WHERE bid = ?''',
                        (bid_price, bid_value, status, bid.id, ))
            conn.commit()

    conn.close()
    return