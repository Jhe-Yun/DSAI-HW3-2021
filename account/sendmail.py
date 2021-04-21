# -*- coding: UTF-8 -*-

import os
import random
import smtplib
import string
import pandas as pd
import subprocess
from dotenv import load_dotenv
from pathlib import Path
from email.mime.text import MIMEText
from email.header import Header
from datetime import datetime

load_dotenv(Path("../private/.env"))
mail_server = "smtp.gmail.com"
mail_port = 587
sender_mail = os.getenv("email")
sender_password = os.getenv("password")
student = pd.read_csv("data.csv")
password = list()

smtp = smtplib.SMTP(mail_server, mail_port)
smtp.ehlo()
smtp.starttls()
smtp.login(sender_mail, sender_password)

alphabet = list(string.ascii_letters + string.digits)
random.seed(datetime.now())

for index in range(len(student)):
    temp = "".join(random.sample(alphabet, k=random.randint(8, 10)))
    password.append(temp)
    name, sid, mail = student.loc[index, ["Name 1", "Student ID 1", "電子郵件地址"]]
    content = "<p>" + name + " 您好: <br />" +\
              "以下為您的 SFTP 連線方式</p>" +\
              "<p>Host: 140.116.247.123<br/>" +\
              "Port: 22<br/>" +\
              "Username: " + sid + "<br/>" +\
              "Password: " + temp + "</p>" +\
              "<p>若您有任何無法連線等問題，麻煩儘速與助教聯繫，謝謝!</p>"
    message = MIMEText(content, "html", "utf-8")
    message["Subject"] = Header("[DSAI-HW3] SFTP 連線方式通知信", "utf-8")
    message["From"] = Header("netdb", "utf-8")
    message["To"] = Header(mail)

    status = smtp.sendmail(sender_mail, mail, message.as_string())
    if status == {}:
        print(f"{sid} {name} {temp} success")
    else:
        print(f"{sid} {name} {temp} error")

student["password"] = password
student.to_csv("info.csv", index=False)

smtp.quit()


with open("user_info.txt", "w") as output:
    uid = 1100
    for index in range(len(student)):
        user = student.at[index, "Student ID 1"] + ":x:" + str(uid) + ":1015::/home/dsai1092:\n"
        output.write(user)
        uid += 1

with open("user_password.txt", "w") as output:
    for index in range(len(student)):
        user = student.at[index, "Student ID 1"] + ":" + student.at[index, "password"] + "\n"
        output.write(user)