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
from database import bids_insert, bids_get, student_sync
from match import match


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

    process = subprocess.run(f"pipenv run python main.py\
                               --consumption ../../input/validation/consumption/1_{file_box[student_id]['agent']}_{args[0]}.csv\
                               --generation ../../input/validation/generation/2_{file_box[student_id]['agent']}_{args[0]}.csv\
                               --output ../../output/{file_box[student_id]['filename']}.csv",
                             shell=True, cwd=code_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if process.returncode != 0:
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
    pr = subprocess.run(f"rm {root_path}*",
                        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if pr.returncode != 0:
        logger.error(f"{pr.stderr}")
        return
    logger.success(f"delete student output file return_code: {pr.returncode}")


def exchange_to_csv(mid, upload_df):
    """
    write student bid exchange data from sql db to ./download (FTP)

    Parameter:
    - mid
    - upload_df(dataframe)
    """

    for student_id in upload_df.index:
        if upload_df.at[student_id, "status"] == "P":
            data = bids_get(bidder=student_id)

            dir_path = f"{os.getenv('download_url')}{student_id}"
            file_path = f"{dir_path}/exchange-{mid}.csv"
            if not os.path.isdir(dir_path):
                process = subprocess.run(f"mkdir -p {dir_path}/",
                                         shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if process.returncode != 0:
                    logger.error(f"created {student_id}/ dir error")
                    continue
                logger.success(f"success created {student_id}/ dir")

            data = data.drop(columns=["bid", "agent"])
            data.to_csv(file_path, index=False)
            logger.success(f"success wrote data to {file_path}")



def period_transaction(file_box, upload_df):
    # # backup
    # ### select agent ###
    # for agent in range(len(file_box.keys())):
    #     ### for loop days ###
    #     multi_processing(execute_student_code, file_box, agent)
    #     logger.info(f"for the {agent} time, all student code have been executed")
    #     check_student_code(upload_df)
    #     ### for 24 round ###

    mid = upload_df.iat[0, -1]
    student_list = [i for i in file_box.keys()]
    agent_index = random.sample([i for i in range(50)], k=len(student_list))
    logger.info(f"agent_index: {agent_index}")

    multi_processing(student_build_env, file_box)

    for flag in range(len(student_list)):

        start_time = datetime.strptime("2018-08-25 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime("2018-08-31 00:00:00", "%Y-%m-%d %H:%M:%S")
        while start_time < end_time:

            for index, student in enumerate(student_list):
                file_box[student]["flag"] = flag
                file_box[student]["agent"] = agent_index[(index + flag) % len(student_list)]

            interval = start_time.strftime("%m%d") + (start_time + timedelta(hours=167)).strftime("%m%d")
            multi_processing(execute_student_code, file_box, interval)
            logger.info(f"{interval}")

            check_student_code(upload_df)

            for hour in range(24):
                match_time = start_time + timedelta(hours=(7 * 24 + hour))
                logger.info(f"match_time: {match_time}")
                data = bids_get(time=match_time.strftime("%Y-%m-%d %H:%M:%S"), flag=flag)
                match(mid, data)
            ###
            # for loop
            # match.py (every one hour)
            # bill.py (mid, time, flag)
            # wait()
            ###

            start_time += timedelta(days=1)

        logger.info(f"The {flag}th tansaction has been compeleted, with {len(student_list)} participants.")

    multi_processing(student_remove_env, file_box)
    exchange_to_csv(mid, upload_df)


def multi_processing(func, file_box, *args):
    with mp.Pool(mp.cpu_count()-2) as pool:
        for student_id in file_box.keys():
            pool.apply_async(
                func,
                (student_id, file_box, *args)
            )
        pool.close()
        pool.join()


def routine(mid, upload_page, student_page, upload_root_path):

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

    upload_page.set_dataframe(upload_df.iloc[:, :-1], start="A2", copy_head=False, copy_index=True, nan='')
    logger.info("update upload page")

    upload_page.update_value("J3", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    logger.info("update time")

    status_code = student_sync(copy.deepcopy(upload_df))
    if status_code == 400:
        logger.error("db sync student error")
        return

    ####################################
    # print upload_df
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #     print(upload_df)

    # os.path.isfile(file_path)
    # os.system(f"sudo cp /data/dsai1092/upload/{student_id}.zip /home/netdb/dsai-server/data/{student_id}.zip")
    # logger.info(f"cp {student_id} file")
    # os.system(f"sudo unzip ./data/{student_id}.zip -d ./data/{student_id}/")
    # logger.info(f"unzip {student_id} file")
    # os.system(f"sudo rm ./data/{student_id}.zip")