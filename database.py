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


def match_update(time):

    conn, cur = db_connect()
    cur.execute(f'''UPDATE match
                    SET execute_time = {time}
                    WHERE mid = {mid}''')
    conn.commit()
    conn.close()
    return


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


def bids_update(time, flag, buys, sells):
    """
    update bid status after match

    Parameter:
    - time
    - flag
    - buys
    - sells
    """

    conn, cur = db_connect()
    trades = {trade.id: {"volume": float("{:.2f}".format(trade.value)),
                          "price": float("{:.2f}".format(trade.price))}
              for trade in buys+sells}
    logger.debug(f"trades: {trades}")

    data = db_get("bids", time=time, flag=flag)
    data.set_index("bid", inplace=True)
    data[["trade_volume", "trade_price"]] = (
        0,
        trades[list(trades.keys())[0]]["price"] if trades else -1
    )

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
    params = [bid for bid in data.itertuples(index=False, name=None)]
    logger.debug(params)
    cur.executemany('''UPDATE bids
                       SET trade_price = ?, trade_volume = ?, status = ?
                       WHERE bid = ?''', params)
    conn.commit()
    logger.success(f"success wrote data to db")

    conn.close()
    return


def bill_insert(bills):
    """
    insert student electricity bill by per hour

    Parameter:

    """

    conn, cur = db_connect()

    [bill.append(mid) for bill in bills]
    logger.info(f"bills: {bills}")
    cur.executemany('''INSERT INTO bill(flag, sid, time, money, mid)
                       VALUES (?, ?, ?, ?, ?)''', bills)
    conn.commit()
    logger.success("bill success wrote to db")

    conn.close()
    return


def db_get(table, **kwargs):
    """
    [Beta]
    for bids_get() and bill_get()

    Parameter:
    - table
    - **kwargs
    """

    conn, cur = db_connect()

    query = f'''SELECT *
                FROM {table}
                WHERE mid = {mid} and ''' + " and ".join(param + " = :" + param for param in kwargs)
    data = pd.read_sql(query, conn, params=kwargs)
    data["time"] = data["time"].map(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))

    conn.close()
    return data
