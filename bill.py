import os
import sys
import pandas as pd
import random
from loguru import logger
from database import bill_insert, db_get
from datetime import timedelta


def calculate_hour_bill(time, flag, file_box, upload_df):
    """

    Parameter:
    - time: per hour
    - flag
    - file_box: get student agent number
    - upload_df: get all student_id
    """

    # agent_list = [file_box[i]["agent"] for i in file_box.keys()]
    exchange_data = db_get("bids", time=time, flag=flag)
    taipower = 2.53
    bills = []

    for student_id in upload_df.index:
        money = 0
        if student_id in file_box.keys() and upload_df.at[student_id, "status"] == "P":
            agent = file_box[student_id]["agent"]
        else:
            # logger.info(f"{student_id} no bills")
            continue
        # agent = (file_box[student_id]["agent"]
        #         if student_id in file_box.keys() # 至少有傳檔案上來 (有分配到 agent)
        #         else random.randint(0, 49))      # 一開始就沒傳檔案 (沒有 agent)

        # truth data
        truth_path = f"{os.getenv('truth_url')}{os.getenv('phase')}/target{agent}.csv"
        truth_data = pd.read_csv(truth_path, header=None, index_col=0)
        generation, consumption = truth_data.loc[time, :]

        # exchage data
        bids = exchange_data[(exchange_data["status"] != "未成交") &
                             (exchange_data["bidder"] == student_id)].reset_index()

        volume = 0
        if not bids.empty:
            buys = bids[bids["action"] == "buy"]
            sells = bids[bids["action"] == "sell"]
            volume = sum(buys["trade_volume"]) - sum(sells["trade_volume"]) + generation
            money += (((volume - generation) * bids.at[0, "trade_price"])
                      if volume >= 0
                      else (generation * bids.at[0, "trade_price"] * (-1)))
        volume -= consumption
        if volume < 0:
            money += (volume * taipower * (-1))

        money = float("{:.2f}".format(money))
        if money != 0:
            bills.append([flag, student_id, time, money])

    return bills
    # bill_insert(bills)


def calculate_total_bill_rank(upload_df):
    """
    calculate all student total electricity bill

    Parameter:
    - upload_df(dataframe)
    """

    for student_id in upload_df.index:
        data = db_get("bill", sid=student_id)
        bill = (float("{:.2f}".format(sum(data["money"])))
                if upload_df.loc[student_id, "status"] == "P"
                else sys.maxsize)
        upload_df.at[student_id, "bill"] = bill

        # rank
        upload_df["rank"] = upload_df["bill"].rank(method="min", ascending=True)

    return
