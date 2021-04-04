import sqlite3
from config import get_time
from schema import Match
from loguru import logger

def match_initial(conn, cur):
    time = get_time()

    cur.execute("INSERT INTO match(time) VALUES (?)", (time,))
    conn.commit()
    test = cur.execute("SELECT * FROM match").fetchall()
    logger.success("insert match info success")

    mid = cur.execute("SELECT mid FROM match WHERE time = ?", (time, )).fetchone()[0]
    logger.info(f"mid: {mid}")

    return mid