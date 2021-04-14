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


def bids_get(**kwargs):
    """
    for match.py and bill.py

    Parameter:
    - **kwargs
        - time(%Y-%m-%d %H:%M:%S)
        - flag
        or
        - student_id
    """

    conn, cur = db_connect()

    query = f'''SELECT *
                FROM bids
                WHERE mid = {mid} and ''' + " and ".join(param + " = :" + param for param in kwargs)
    data = pd.read_sql(query, conn, params=kwargs)

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
    trades = {trade.id: {"volume": float("{:.2f}".format(trade.value)),
                          "price": float("{:.2f}".format(trade.price))}
              for trade in buys+sells}
    logger.debug(f"trades: {trades}")

    data = bids_get(mid=mid)
    data.set_index("bid", inplace=True)
    data[["trade_volume", "trade_price"]] = -1, -1

    for index, row in data.iterrows():
        if not index in trades.keys():
            data.at[index, "status"] = "未成交"
            continue
        data.at[index, "status"] = ("完全成交"
                                    if row.target_volume == trades[index]["volume"]
                                    else "部分成交")
        data.loc[index, ["trade_volume", "trade_price"]] = [trades[index]["volume"], trades[index]["price"]]

    data.reset_index(inplace=True)
    data = data[["trade_price", "trade_volume", "status", "bid"]]
    cur.executemany('''UPDATE bids
                       SET trade_price = ?, trade_volume = ?, status = ?
                       WHERE bid = ?''',
                    data.values.tolist())
    conn.commit()
    logger.success(f"success wrote data to db")

    conn.close()
    return
