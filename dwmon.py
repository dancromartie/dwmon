import datetime
import logging
import os
import math
import random
import re
import sqlite3
import time

import your_orgs_row_getter

DB_NAME = "dwmon_fake.db"
CONFIGS_FOLDER = "./checker_configs"

def parse_config_file(checker_name):
    """Parses our custom config format"""
    query_sentinel = "__QUERY__"
    source_sentinel = "__SOURCE__"
    requirements_sentinel = "__REQUIREMENTS__"
    unique_key_sentinel = "dwmon_unique_key"
    timestamp_sentinel = "dwmon_timestamp"

    with open(CONFIGS_FOLDER + "/" + checker_name + ".dwmon") as f_handle:
        config_as_string = f_handle.read()

    assert query_sentinel in config_as_string, "Expected %s" \
        % query_sentinel
    assert requirements_sentinel in config_as_string, "Expected %s" \
        % requirements_sentinel
    assert unique_key_sentinel in config_as_string, "Expected %s" \
        % unique_key_sentinel
    assert timestamp_sentinel in config_as_string, "Expected %s" \
        % timestamp_sentinel
    assert source_sentinel in config_as_string, "Expected %s" \
        % source_sentinel

    regex_string =  query_sentinel + "(.*)" + requirements_sentinel \
        + "(.*)" + source_sentinel + "(.*)"
    query_search = re.search(
        regex_string,
        config_as_string,
        re.DOTALL
    )
    assert query_search, "Config parse failed for some reason"

    query = query_search.group(1).strip()
    requirements_sets = query_search.group(2).strip().split("\n")
    query_source = query_search.group(3).strip()

    requirements = [parse_requirements(x) for x in requirements_sets]
    query_details = {"query": query, "source": query_source}
    return (query_details, requirements)


def _get_rows_from_query(query, data):
    """Just returns tuples of rows in memory"""
    to_return = []
    db_conn = sqlite3.connect(DB_NAME)
    results = db_conn.cursor().execute(query, data)
    for result in results:
        to_return.append(result)
    db_conn.close()
    return to_return


def _write_query(query, data, many=False):
    """wrapper around writes"""
    db_conn = sqlite3.connect(DB_NAME)
    if many:
        db_conn.cursor().executemany(query, data)
    else:
        db_conn.cursor().execute(query, data)
    db_conn.commit()
    db_conn.close()


def store_results(checker_name, results):
    """Merge the passed results with all existing results"""
    id_query = """
        SELECT unique_id FROM results WHERE checker = ?
    """
    existing_ids = [
        x[0] for x in _get_rows_from_query(id_query, (checker_name,))
    ]
    existing_ids = set(existing_ids)
    insert_query = """
        INSERT INTO results (checker, unique_id, timestamp)
        VALUES (?, ?, ?)
    """
    to_insert = []
    for row in results:
        id_ = row[0]
        timestamp = row[1]
        if id_ not in existing_ids:
            to_insert.append((checker_name, id_, timestamp))
    _write_query(insert_query, to_insert, many=True)


def log_check(checker_name, minute_epoch):
    assert isinstance(minute_epoch, int)
    insert_query = """
        INSERT INTO checks (checker, timestamp) VALUES (?, ?)
    """
    insert_data = (checker_name, minute_epoch)
    _write_query(insert_query, insert_data)


def create_tables():
    """Sets up tables used internally. Probably should let this work on more
    than just sqlite"""
    results_creation_query = """
        CREATE TABLE results (unique_id text, checker text, timestamp integer)
    """
    results_index_query = """
        CREATE INDEX idx_results_id ON results (unique_id)
    """
    _write_query(results_creation_query, ())
    _write_query(results_index_query, ())

    # I'm avoiding the primary key on replace thing cause postgres doesn't have 
    # that feature :(
    checks_creation_query = """
        CREATE TABLE checks (checker text, timestamp integer)
    """
    checks_index_query = """
        CREATE INDEX idx_checker_key ON checks (checker)
    """
    _write_query(checks_creation_query, ())
    _write_query(checks_index_query, ())


def parse_requirements(requirements_string):
    """
    Given a requirements string (which might contain a bunch of extra stuff),
    find the check time related info in it.
    """

    bad_character_search = re.search(r"[^A-Z\s0-9-]", requirements_string)
    assert not bad_character_search, "Bad characters detected in requirements"

    assert "CHECKHOURS" in requirements_string, "missing CHECKHOURS"
    assert "CHECKMINUTES" in requirements_string, "missing CHECKMINUTES"
    assert "MAXNUM" in requirements_string, "missing MAXNUM"
    assert "MINNUM" in requirements_string, "missing MINNUM"
    assert "LOOKBACKSECONDS" in requirements_string, "missing LOOKBACKSECONDS"

    has_day_of_week_info = " WEEKENDS" in requirements_string or \
        " WEEKDAYS" in requirements_string
    assert has_day_of_week_info, "No weekend/weekday info supplied"

    check_hours_search = re.search(
        r"CHECKHOURS(\d+)-(\d+)",
        requirements_string
    )
    assert check_hours_search, "Couldn't parse hours info"
    check_hours_lower = int(check_hours_search.group(1))
    check_hours_upper = int(check_hours_search.group(2))

    check_minutes_search = re.search(
        r"CHECKMINUTES(\d+)-(\d+)",
        requirements_string
    )
    assert check_minutes_search, "Couldn't parse minutes info"
    check_minutes_lower = int(check_minutes_search.group(1))
    check_minutes_upper = int(check_minutes_search.group(2))

    assert check_hours_lower <= check_hours_upper, \
        "bad hours relationship"
    assert check_minutes_lower <= check_minutes_upper, \
        "bad minutes relationship"
    assert check_hours_lower >= 0 and check_hours_upper <= 23, \
        "out of range hours specified"
    assert check_minutes_lower >= 0 and check_minutes_upper <= 59, \
        "out of range minutes specified"

    include_weekends = "WEEKENDS" in requirements_string
    include_weekdays = "WEEKDAYS" in requirements_string

    min_num_search = re.search(
        r"MINNUM(\d+)",
        requirements_string
    )
    min_num = int(min_num_search.group(1))

    max_num_search = re.search(
        r"MAXNUM(\d+)",
        requirements_string
    )
    max_num = int(max_num_search.group(1))

    lookback_seconds_search = re.search(
        r"LOOKBACKSECONDS(\d+)",
        requirements_string
    )
    lookback_seconds = int(lookback_seconds_search.group(1))

    assert lookback_seconds > 0
    assert min_num >= 0 and min_num <= max_num, "bad minnum/maxnum"

    parsed = {
        "check_hours_lower": check_hours_lower,
        "check_hours_upper": check_hours_upper,
        "check_minutes_lower": check_minutes_lower,
        "check_minutes_upper": check_minutes_upper,
        "include_weekdays": include_weekdays,
        "include_weekends": include_weekends,
        "min_num": min_num,
        "max_num": max_num,
        "lookback_seconds": lookback_seconds,
    }
    return parsed


def matches_time_pattern(requirements, epoch):
    """
    Checks that an epoch matches the time pattern in the requirements.
    For example, if we have epoch 12345678900, we can check if this is 
    indeed in the 23rd minute of the first hour of the day on a weekend.
    """
    datetime_obj = datetime.datetime.fromtimestamp(epoch)

    if datetime_obj.minute < requirements["check_minutes_lower"]:
        return False
    if datetime_obj.minute > requirements["check_minutes_upper"]:
        return False
    if datetime_obj.hour < requirements["check_hours_lower"]:
        return False
    if datetime_obj.hour > requirements["check_hours_upper"]:
        return False

    # Monday is 0, sunday is 6
    day_of_week = datetime_obj.weekday()

    matches_day_of_week = False
    if requirements["include_weekdays"]:
        if day_of_week in [0, 1, 2, 3, 4]:
            matches_day_of_week = True

    if requirements["include_weekends"]:
        if datetime_obj.weekday() in [5, 6]:
            matches_day_of_week = True

    if not matches_day_of_week:
        return False

    return True


def do_multiple_history_check(checker_name, query_details, requirements):
    """
    Check that events recorded match the requirements in the config
    Args:
    requirements -- ONE set of parsed requirements (not all)
    """

    assert "select" in query_details["query"].lower()
    assert len(checker_name) < 100
    assert isinstance(requirements, dict)

    # Go through a bunch of recent minutes, check for cron eligibility
    # If we do a check, mark that so we don't do it again
    # Get the current epoch and round it down to the nearest minute
    current_epoch = int(time.time())
    minute_epoch_max = int(math.floor(current_epoch / 60) * 60)
    # If the epoch is 1 trillion, say, let's pretend that it's 
    # 1trillion - 60, 1trillion - 120, 1trillion - 180, etc.
    # We might have some lag in our actual cron running these checks, 
    # so we always want to say "would we have alerted at this time with the 
    # data we have now if we ran the cron then?

    minute_epochs_to_check = [minute_epoch_max]
    num_minutes_to_check = int(
        math.ceil(requirements['lookback_seconds']/ 60) * 10)
    minute_marker = minute_epoch_max
    for i in range(num_minutes_to_check):
        minute_marker -= 60
        minute_epochs_to_check.append(minute_marker)

    previous_checks_query = """
        SELECT max(timestamp) FROM checks
        WHERE checker = ?
        ORDER BY timestamp DESC
        LIMIT 10000
    """
    previous_check_results = _get_rows_from_query(
        previous_checks_query,
        (checker_name,)
    )
    time_of_most_recent_check = previous_check_results[0][0]
    eligible_minutes = [
        x for x in minute_epochs_to_check \
            if matches_time_pattern(requirements, x) and x > time_of_most_recent_check
    ]
    all_new_checks = []
    if eligible_minutes:
        # Refresh results, just once if we have reason to check
        rows = your_orgs_row_getter._get_rows_from_query(query_details, ())
        store_results(checker_name, rows)

        filtered_minutes = [] 
        for elig_min in eligible_minutes:
            logging.info("eligible minute is %s minutes ago" \
                % ((int(time.time()) - elig_min) / 60))
            logging.info("Checking history for %s" % checker_name)
            check_status = do_single_history_check(
                checker_name,
                elig_min,
                requirements
            )
            assert check_status in ["GOOD", "BAD"]
            logging.info("checker: %s, status: %s, check_time: %s" \
                % (checker_name, check_status, elig_min))
            all_new_checks.append(check_status)
            log_check(checker_name, elig_min)
    return all_new_checks


def do_single_history_check(checker_name, minute_epoch, requirements):
    """
    Args:
    minute_epoch - the epoch at the start of the (hypothetical) minute
    """
    assert isinstance(minute_epoch, int)
    lookback_seconds = requirements["lookback_seconds"]
    seconds_lower = minute_epoch - lookback_seconds
    seconds_upper = minute_epoch
    events_query = """
        SELECT count(1) FROM results WHERE checker = ?
        AND timestamp BETWEEN ? and ?
    """
    events_query_data = (checker_name, seconds_lower, seconds_upper)
    rows = _get_rows_from_query(events_query, events_query_data)
    event_count = rows[0][0]
    print "Found %s events in the time window" % event_count
    if event_count < requirements["min_num"]:
        return "BAD"
    if event_count > requirements["max_num"]:
        return "BAD"
    return "GOOD"


def get_checker_names():
    """
    Go through the config directory and figure out the checker names 
    from the files there.
    """
    names = []
    files = os.listdir(CONFIGS_FOLDER)
    for f in files:
        if f.endswith(".dwmon"):
            names.append(f.replace(".dwmon", ""))
    assert names, "No checker names found.  Is the configs dir empty?"
    return names


def check_all():
    """
    Get all checker names and do all their checks
    """
    checker_names = get_checker_names()
    for checker_name in checker_names:
        query_details, requirements = parse_config_file(checker_name)
        for req in requirements:
            statuses = do_multiple_history_check(checker_name, query_details, req)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)-8s %(levelname)-8s %(message)s'
    )
    while True:
        check_all()
        logging.info("Sleeping...")
        time.sleep(60)
