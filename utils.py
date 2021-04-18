import subprocess
import os
import re
import pandas as pd
import time
import copy
import random
import multiprocessing as mp
from datetime import datetime, timedelta
from loguru import logger
from database import bids_insert, db_get, student_sync
from match import match
from bill import calculate_hour_bill, calculate_total_bill_rank


def sync_student(upload_page, student_page):
    row_num = len(student_page.get_col(1, include_tailing_empty=False))
    data = student_page.get_values((2, 1), (row_num, 2))
    upload_page.update_values(crange=(2, 1), values=data)


def sync_upload_page(df, file_box):
    df["status"] = "F"
    for student_id in df.index:
        if student_id in file_box.keys():
            df.loc[student_id, ["filename", "last time"]] = (
                file_box[student_id]["filename"],
                datetime.fromtimestamp(os.path.getmtime(file_box[student_id]["path"])) \
                        .strftime("%Y-%m-%d %H:%M:%S"),
            )


def file_delete(student_file_list, root_path):
    for root, dirs, files in os.walk(root_path):
        for filename in files:
            temp = os.path.join(root, filename)
            if not temp in student_file_list:
                os.system(f"echo 'y' | rm -r {temp}")
                logger.info(f"delete file {temp}")

    # delete previous server data
    process = subprocess.run("rm -r ./data/code/*", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.returncode != 0:
        logger.error(f"delete server code data error: {process.stderr}")
    logger.success("delete server code data")

    # delete bidresult by past 7 days
    process = subprocess.run("rm -r ./data/input/bidresult/*", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.returncode != 0:
        logger.error(f"delete server bidresult error: {process.stderr}")
    logger.success("delete server bidresult data")


def file_manage(student_list, root_path):
    # according to student_id classification
    file_box = dict()
    for root, dirs, files in os.walk(root_path):
        for filename in files:
            temp = filename.split("-")
            if len(temp) == 1:
                temp = (temp[0].split(".")[0] + "-1" + temp[0].split(".")[1]).split('-')

            temp[0] = temp[0].upper()
            file_box[temp[0]] = file_box.get(temp[0], {"version": list(), "path": list(), "filename": list()})
            temp[1] = float(re.findall(r"\d+\.?\d*", temp[1])[0])
            file_box[temp[0]]["version"].append(temp[1])
            file_box[temp[0]]["path"].append(os.path.join(root, filename))
            file_box[temp[0]]["filename"].append(filename.split(".zip")[0])

    # each student leaves a file
    for index in file_box.keys():
        # delete student_id when not exist in list
        if not index in student_list:
            del file_box[index]
        # select file max version
        pre = file_box[index]
        latest_index = pre["version"].index(max(pre["version"]))
        pre["version"] = pre["version"][latest_index:latest_index+1][0]
        pre["path"] = pre["path"][latest_index:latest_index+1][0]
        pre["filename"] = pre["filename"][latest_index:latest_index+1][0]
        logger.info(f"student: {index}, max_version: {pre['version']}, path: {pre['path']}, filename: {pre['filename']}")

    # delete unnecessary files
    file_delete([file_box[index]["path"] for index in file_box.keys()], root_path)

    return file_box


def unzip_file(student_id, file_box):
    try:
        server_file_path = f"./data/code/{file_box[student_id]['filename']}"
        if not os.path.isdir(server_file_path):
            os.system(f"sudo unzip {file_box[student_id]['path']} -d {server_file_path}")
            logger.info(f"success unzip {file_box[student_id]['filename']} file")
        else:
            logger.error(f"{file_box[student_id]['filename']} exist")
    except Exception as e:
        logger.error(e)


def student_build_env(student_id, file_box, *args):
    code_path = f"./data/code/{file_box[student_id]['filename']}/"
    process = subprocess.run("pipenv install",
                             shell=True, cwd=code_path,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.returncode != 0:
        logger.error(f"{student_id} env: {process.stderr}")
        return
    logger.success(f"{student_id} env builded")
    return


def student_remove_env(student_id, file_box, *args):
    code_path = f"./data/code/{file_box[student_id]['filename']}/"
    process = subprocess.run("pipenv --rm", shell=True, cwd=code_path,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.returncode != 0:
        logger.error(f"{student_id} env: {process.stderr}")
        return
    logger.success(f"{student_id} env removed")
    return


def execute_student_code(student_id, file_box, *args):
    code_path = f"./data/code/{file_box[student_id]['filename']}/"

    try:
        process = subprocess.run(f"pipenv run python main.py\
                                 --consumption ../../input/{os.getenv('phase')}/consumption/1_{file_box[student_id]['agent']}_{args[0]}.csv\
                                 --generation ../../input/{os.getenv('phase')}/generation/2_{file_box[student_id]['agent']}_{args[0]}.csv\
                                 --bidresult ../../input/bidresult/student/{student_id}/{args[0]}.csv\
                                 --output ../../output/{file_box[student_id]['filename']}.csv",
                                 shell=True, cwd=code_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    except subprocess.TimeoutExpired:
        logger.error(f"{student_id} code time out.")
        return
    except Exception as e:
        logger.error(f"{student_id} code error: {process.stderr}")
        return
    logger.success(f"{student_id} code successfully executed")
    logger.info(file_box)

    bids_insert(student_id,
                file_box[student_id]["filename"],
                file_box[student_id]["flag"],
                file_box[student_id]["agent"],
                "2018" + args[0][-4:])

    return


def check_student_code(df):
    """
    whether there is an output file to determine the status

    parameter:
    - df: upload page dataframe(this is shallow copy)
    """

    root_path = "./data/output/"
    df["status"] = "F"
    for student_id, filename in zip(df.index, df["filename"]):
        filename += ".csv"
        df.loc[student_id, "status"] = (
            "P"
            if os.path.isfile(f"{root_path}{filename}")
            else "F"
        )
    success_num = len(df[df["status"] == "P"])
    logger.info(f"number of success code: {success_num}")

    pr = subprocess.run(f"sudo rm {root_path}*",
                        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if pr.returncode != 0:
        logger.error(f"{pr.stderr}")
        return
    logger.success(f"delete student output file return_code: {pr.returncode}")
    return success_num


def bidresult_to_csv(mid, upload_df):
    """
    write student bidresult data from sql db to ./download (FTP)

    Parameter:
    - mid
    - upload_df(dataframe)
    """

    for student_id in upload_df.index:
        if upload_df.at[student_id, "status"] == "P":
            data = db_get("bids", bidder=student_id)

            dir_path = f"{os.getenv('download_url')}student/{student_id}"
            file_path = f"{dir_path}/bidresult-{mid}.csv"
            if not os.path.isdir(dir_path):
                process = os.makedirs(f"{dir_path}")

            data.drop(columns=["bid", "agent"], inplace=True)
            data.to_csv(file_path, index=False)
            logger.success(f"success wrote data to {file_path}")


def bill_to_csv(mid, flag_num, upload_df):
    """
    write student electricity bill from sql db to ./download (FTP)

    Parameter:
    - mid
    - flag_num
    - upload_df(dataframe)
    """

    for student_id in upload_df.index:
        if upload_df.at[student_id, "status"] == "P":
            data = db_get("bill", sid=student_id)
            data.drop(columns=["id"], inplace=True)
            data.rename({"sid": "bidder"}, axis=1, inplace=True)

            dir_path = f"{os.getenv('download_url')}student/{student_id}/"
            file_path = f"{dir_path}/bill-{mid}.csv"
            if not os.path.isdir(dir_path):
                process = os.makedirs(f"{dir_path}")

            day_data = pd.DataFrame(columns=["mid", "flag", "bidder", "time", "money"])
            for flag in range(flag_num):
                start_time = datetime.strptime(os.getenv("bill_start_time"), "%Y-%m-%d %H:%M:%S")
                end_time = datetime.strptime(os.getenv("bill_end_time"), "%Y-%m-%d %H:%M:%S")
                while start_time < end_time:
                    temp_end_time = start_time + timedelta(days=1)
                    row = data[(data["time"] >= start_time) & (data["time"] < temp_end_time) & (data["flag"] == flag)]
                    row = pd.DataFrame([[mid, flag, student_id, start_time.strftime("%Y-%m-%d"), "{:.2f}".format(sum(row["money"]))]],
                                    columns=["mid", "flag", "bidder", "time", "money"])
                    day_data = day_data.append(row, ignore_index=True)
                    start_time += timedelta(days=1)

            day_data.to_csv(file_path, index=False)
            logger.success(f"success wrote data to {file_path}")


def beta_bidresult_to_csv(student_id, file_box, *args):
    """
    per student get bidresult data from sql to csv for the past 7 days

    Parameter:
    - student_id
    - file_box
    - *args
        - path
        - interval
        - start_time
        - temp_end_time
    """

    data = (db_get("bids", bidder=student_id)
            if not args[1]
            else db_get("bids", bidder=student_id, flag=file_box[student_id]["flag"]))

    dir_path = f"{os.getenv(args[0])}student/{student_id}/"
    file_path = f"{dir_path}{args[1]}.csv"
    if not os.path.isdir(dir_path):
        process = os.makedirs(f"{dir_path}")

    # filter data by start_time and end_time
    if args[2] and args[3]:
        # data["time"] = data["time"].map(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
        data = data[(data["time"] >= args[2]) & (data["time"] < args[3])]

    data.drop(columns=["bid", "agent"], inplace=True)
    data.to_csv(file_path, index=False)
    logger.success(f"success wrote data to {file_path}")


def period_transaction(file_box, upload_df):

    mid = upload_df.iat[0, -1]
    # student_list = [i for i in file_box.keys()]
    agent_index = random.sample([i for i in range(50)], k=len(file_box))
    logger.info(f"agent_index: {agent_index}")

    multi_processing(student_build_env, file_box)

    for flag in range(len(file_box)):

        start_time = datetime.strptime(os.getenv("trans_start_time"), "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(os.getenv("trans_end_time"), "%Y-%m-%d %H:%M:%S")
        while start_time < end_time:

            for index, student in enumerate(file_box.keys()):
                file_box[student]["flag"] = flag
                file_box[student]["agent"] = agent_index[(index + flag) % len(file_box)]

            temp_end_time = start_time + timedelta(hours=167)
            interval = start_time.strftime("%m%d") + temp_end_time.strftime("%m%d")

            # generate bidresult data
            multi_processing(beta_bidresult_to_csv, file_box, "input_bidresult_url", interval, start_time, temp_end_time)

            multi_processing(execute_student_code, file_box, interval)
            logger.info(f"{interval}")

            ### 這個 student_num 每次可能都不同 ###
            success_num = check_student_code(upload_df)

            for hour in range(24):
                match_time = (start_time + timedelta(hours=(7 * 24 + hour))).strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"match_time: {match_time}")
                match(match_time, flag)
                calculate_hour_bill(match_time, flag, file_box, upload_df)

            start_time += timedelta(days=1)

        logger.info(f"The {flag}th tansaction has been compeleted, with {len(file_box.keys())} participants.")

    multi_processing(student_remove_env, file_box)
    bidresult_to_csv(mid, upload_df)
    bill_to_csv(mid, len(file_box), upload_df)


def update_information(mid, student_num):
    """
    update information page by bids database

    Parameter:
    - mid
    - student_num
    """

    info_list = list()
    for flag in range(student_num):

        start_time = datetime.strptime(os.getenv("bill_start_time"), "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(os.getenv("bill_end_time"), "%Y-%m-%d %H:%M:%S")
        while start_time < end_time:
            data = db_get("bids", time=start_time.strftime("%Y-%m-%d %H:%M:%S"), flag=flag)

            target_buy_volume = "{:.2f}".format(sum(data[data["action"] == "buy"]["target_volume"]))
            target_sell_volume = "{:.2f}".format(sum(data[data["action"] == "sell"]["target_volume"]))
            target_num = len(data)
            trade_price = -1 if data.empty else data.loc[0, "trade_price"]
            trade_data = data[data["status"] != "未成交"]
            trade_volume = "{:.2f}".format(sum(trade_data["trade_volume"]))
            trade_num = len(trade_data)

            info_list.append([flag, start_time,
                              target_buy_volume, target_sell_volume, target_num,
                              trade_price, trade_volume, trade_num])

            start_time += timedelta(hours=1)

    info_df = pd.DataFrame(info_list,
                           columns=["flag", "start_time",
                                    "target_buy_volume", "target_sell_volume", "target_num",
                                    "trade_price", "trade_volume", "trade_num"])
    info_df.to_csv(f"{os.getenv('download_url')}information/info-{mid}.csv", index=False)
    logger.info("success info data to ftp")
    return info_df


def multi_processing(func, file_box, *args):
    with mp.Pool(mp.cpu_count()-2) as pool:
        for student_id in file_box.keys():
            pool.apply_async(
                func,
                (student_id, file_box, *args)
            )
        pool.close()
        pool.join()


def routine(mid, upload_page, student_page, info_page, upload_root_path):

    sync_student(upload_page, student_page)
    logger.info("updated student ID")

    col_num = len(upload_page.get_row(1, include_tailing_empty=False))
    row_num = len(upload_page.get_col(1, include_tailing_empty=False))
    upload_df = upload_page.get_as_df(index_column=1,
                                      end=(row_num, col_num),
                                      numerize=False,
                                      include_tailing_empty=False)
    upload_df.loc[:, ["status", "filename", "last time", "bill", "rank"]] = ""
    upload_df.loc[:, "mid"] = mid
    logger.info("get upload page")

    file_box = file_manage(list(upload_df.index), upload_root_path)
    logger.debug(file_box)
    sync_upload_page(upload_df, file_box)
    logger.info("sync upload_df")

    multi_processing(unzip_file, file_box)
    logger.info("unzip all student file")

    period_transaction(file_box, upload_df)
    logger.info("all matchs are done")

    calculate_total_bill_rank(upload_df)
    logger.info("update all student bill and rank")

    upload_page.set_dataframe(upload_df.iloc[:, :-1], start="A2", copy_head=False, copy_index=True, nan='')
    logger.info("update upload page")

    upload_page.update_value("J3", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    logger.info("update time")

    info_df = update_information(mid, len(file_box))
    info_page.set_dataframe(info_df, start="A2", copy_head=False, nan='')
    logger.info("update info page")

    info_page.update_value("K3", mid)
    logger.info("update info mid")

    info_page.update_value("K4", len(file_box))
    logger.info("update info student num")

    status_code = student_sync(copy.deepcopy(upload_df))
    if status_code == 400:
        logger.error("db sync student error")
        return
