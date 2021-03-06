"""
This module is designed to get you started with some fake records/tables
that you can write queries against for testing etc.
"""

import random
import sqlite3
import time

import config

CONN = sqlite3.connect(config.SQLITE_DB_NAME)


def fake_id():
    """
    Makes a bs random id out of numbers and letters
    """
    letters = [x for x in "abcdefghijklmnop"]
    numbers = [x for x in "1234567890"]
    id_ = ""
    for _ in range(8):
        id_ += random.choice(letters)
        id_ += random.choice(numbers)
    return id_


def fake_an_application():
    """
    Creates a fake record in the applications table
    """
    application_id = fake_id()
    created_date = "2016-02-20"
    stage = random.choice(["won", "lost"])
    insert_query = """
        INSERT INTO applications (application_id, createddate, stage)
        VALUES (?, ?, ?)
    """
    insert_data = (application_id, created_date, stage)
    CONN.cursor().execute(insert_query, insert_data)
    CONN.commit()


def fake_an_execution():
    """
    Creates a fake record in the executions table
    """
    exec_id = fake_id()
    exec_ts = int(time.time())
    model_name = random.choice(["credit", "industry"])
    exec_result = "blahhhhh"
    insert_query = """
        INSERT INTO model_executions (execution_id, ts, model_name, result)
        VALUES (?, ?, ?, ?)
    """
    insert_data = (exec_id, exec_ts, model_name, exec_result)
    CONN.cursor().execute(insert_query, insert_data)
    CONN.commit()


def create_tables():
    """
    Creates some fake tables.
    """
    applications_creation_query = """
        CREATE TABLE applications (
            application_id text,
            createddate text,
            stage text
        )
    """
    model_executions_creation_query = """
        CREATE TABLE model_executions (
            execution_id text,
            ts integer,
            model_name text,
            result text
        )
    """
    CONN.cursor().execute(applications_creation_query, ())
    CONN.cursor().execute(model_executions_creation_query, ())
    CONN.commit()


if __name__ == "__main__":
    while True:
        time.sleep(random.uniform(1, 3))
        fake_an_application()
        time.sleep(random.uniform(1, 3))
        fake_an_execution()
