"""
This is the main script that runs in a loop and performs all checks.
It calls out to some custom defined "row getters" and "check handlers".
It makes determinations about whether or not events are meeting their config'ed behavior.
"""

import datetime
import json
import logging
import os
import math
import re
import sqlite3
import time

import config

DB_NAME = config.SQLITE_DB_NAME
CONFIGS_FOLDER = "./checker_configs"

# Strings used in the config format
QUERY_SENTINEL = "__QUERY__"
SOURCE_SENTINEL = "__SOURCE__"
REQUIREMENTS_SENTINEL = "__REQUIREMENTS__"
EXTRA_SENTINEL = "__EXTRA__"
UNIQUE_KEY_SENTINEL = "dwmon_unique_key"
TIMESTAMP_SENTINEL = "dwmon_timestamp"


def pull_sections_from_config(config_as_string):
    """
    Pulls out certain strings from the config and gives them names.
    """
    regex_string = QUERY_SENTINEL + "(.*)" + REQUIREMENTS_SENTINEL \
        + "(.*)" + SOURCE_SENTINEL + "(.*)" + EXTRA_SENTINEL + "(.*)"
    config_search = re.search(
        regex_string,
        config_as_string,
        re.DOTALL
    )
    assert config_search, "Config parse failed for some reason"
    return {
        "query_string": config_search.group(1).strip(),
        "requirements_string": config_search.group(2).strip(),
        "source_string": config_search.group(3).strip(),
        "extra_string": config_search.group(4).strip()
    }


def parse_config_file(checker_name):
    """Parses our custom config format"""

    with open(CONFIGS_FOLDER + "/" + checker_name + ".dwmon") as f_handle:
        config_as_string = f_handle.read()

    assert QUERY_SENTINEL in config_as_string, "Expected %s" \
        % QUERY_SENTINEL
    assert REQUIREMENTS_SENTINEL in config_as_string, "Expected %s" \
        % REQUIREMENTS_SENTINEL
    assert UNIQUE_KEY_SENTINEL in config_as_string, "Expected %s" \
        % UNIQUE_KEY_SENTINEL
    assert TIMESTAMP_SENTINEL in config_as_string, "Expected %s" \
        % TIMESTAMP_SENTINEL
    assert SOURCE_SENTINEL in config_as_string, "Expected %s" \
        % SOURCE_SENTINEL
    assert EXTRA_SENTINEL in config_as_string, "Expected %s" \
        % EXTRA_SENTINEL

    config_sections = pull_sections_from_config(config_as_string)
    # Whitespace is allowed between requirements lines to help maintain related groups visually
    requirements_sets = re.split(r"\s*\n", config_sections["requirements_string"])
    requirements = [
        parse_requirements(x) for x in requirements_sets  \
            if x.strip() != "" and not x.startswith("#")
    ]

    assert requirements, "No requirements found for checker %s" % checker_name

    query_details = {
        "query": config_sections["query_string"],
        "source": config_sections["source_string"],
    }
    extra_json = json.loads(config_sections["extra_string"])
    return (query_details, requirements, extra_json)


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
    # Don't insert dupes within this batch either
    already_seen = {}
    for row in results:
        id_ = str(row[0])
        timestamp = row[1]
        if id_ not in existing_ids and id_ not in already_seen:
            already_seen[id_] = 1
            to_insert.append((checker_name, id_, timestamp))
    _write_query(insert_query, to_insert, many=True)


def log_check(checker_name, minute_epoch):
    """
    Make a record of us checking this event as of a certain time, so we don't try to do it again.
    """
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


def parse_hours_info(requirements_string):
    """
    Get check time info for hours from a requirements string.
    """
    assert "CHECKHOURS" in requirements_string, "missing CHECKHOURS"
    check_hours_search = re.search(
        r"CHECKHOURS(\d+)-(\d+)",
        requirements_string
    )
    assert check_hours_search, "Couldn't parse hours info"
    check_hours_lower = int(check_hours_search.group(1))
    check_hours_upper = int(check_hours_search.group(2))

    assert check_hours_lower <= check_hours_upper, \
        "bad hours relationship"
    assert check_hours_lower >= 0 and check_hours_upper <= 23, \
        "out of range hours specified"

    return {
        "check_hours_upper": check_hours_upper,
        "check_hours_lower": check_hours_lower
    }


def parse_minutes_info(requirements_string):
    """
    Get check time info for minutes from a requirements string.
    """

    assert "CHECKMINUTES" in requirements_string, "missing CHECKMINUTES"
    # Allow for */20 type notation
    has_star_stuff = False
    check_minutes_search = re.search(
        r"CHECKMINUTES(\d+)-(\d+)",
        requirements_string
    )
    if not check_minutes_search:
        check_minutes_search = re.search(
            r"CHECKMINUTES\*/(\d+)",
            requirements_string
        )
        has_star_stuff = True

    assert check_minutes_search, "Couldn't parse minutes info"

    if not has_star_stuff:
        check_minutes_lower = int(check_minutes_search.group(1))
        check_minutes_upper = int(check_minutes_search.group(2))
        check_minutes_star = None
        assert check_minutes_lower <= check_minutes_upper, \
            "bad minutes relationship"
        assert check_minutes_lower >= 0 and check_minutes_upper <= 59, \
            "out of range minutes specified"
    else:
        check_minutes_lower = None
        check_minutes_upper = None
        check_minutes_star = int(check_minutes_search.group(1))
        assert check_minutes_star < 59 and check_minutes_star > 0
    return {
        "check_minutes_lower": check_minutes_lower,
        "check_minutes_upper": check_minutes_upper,
        "check_minutes_star": check_minutes_star
    }


def parse_day_of_week_info(requirements_string):
    """
    Get check time info for day of week info from a requirements string.
    """
    has_day_of_week_info = " WEEKENDS" in requirements_string or \
        " WEEKDAYS" in requirements_string
    assert has_day_of_week_info, "No weekend/weekday info supplied"
    include_weekends = "WEEKENDS" in requirements_string
    include_weekdays = "WEEKDAYS" in requirements_string
    return {
        "include_weekends": include_weekends,
        "include_weekdays": include_weekdays
    }


def parse_min_max_info(requirements_string):
    """
    Get min max info from a requirements string.
    """
    assert "MAXNUM" in requirements_string, "missing MAXNUM"
    assert "MINNUM" in requirements_string, "missing MINNUM"
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
    assert min_num >= 0 and min_num <= max_num, "bad minnum/maxnum"
    return {
        "min_num": min_num,
        "max_num": max_num
    }


def parse_lookback_info(requirements_string):
    """
    Parse lookback info from a requirements string.  This tells us how far to look back
    into the past.
    """
    assert "LOOKBACKSECONDS" in requirements_string, "missing LOOKBACKSECONDS"
    lookback_seconds_search = re.search(
        r"LOOKBACKSECONDS(\d+)",
        requirements_string
    )
    lookback_seconds = int(lookback_seconds_search.group(1))
    assert lookback_seconds > 0
    return {
        "lookback_seconds": lookback_seconds
    }


def parse_requirements(requirements_string):
    """
    Given a requirements string (which might contain a bunch of extra stuff),
    find the check time related info in it.
    """

    bad_character_search = re.search(r"[^A-Z\s0-9-*/]", requirements_string)
    assert not bad_character_search, "Bad characters detected in requirements"

    minutes_info = parse_minutes_info(requirements_string)
    hours_info = parse_hours_info(requirements_string)
    day_of_week_info = parse_day_of_week_info(requirements_string)
    min_max_info = parse_min_max_info(requirements_string)
    lookback_info = parse_lookback_info(requirements_string)

    parsed = {
        "check_hours_lower": hours_info["check_hours_lower"],
        "check_hours_upper": hours_info["check_hours_upper"],
        "check_minutes_lower": minutes_info["check_minutes_lower"],
        "check_minutes_upper": minutes_info["check_minutes_upper"],
        "check_minutes_star": minutes_info["check_minutes_star"],
        "include_weekdays": day_of_week_info["include_weekdays"],
        "include_weekends": day_of_week_info["include_weekends"],
        "min_num": min_max_info["min_num"],
        "max_num": min_max_info["max_num"],
        "lookback_seconds": lookback_info["lookback_seconds"],
    }
    return parsed


def matches_time_pattern(requirements, epoch):
    """
    Checks that an epoch matches the time pattern in the requirements.
    For example, if we have epoch 12345678900, we can check if this is
    indeed in the 23rd minute of the first hour of the day on a weekend.
    """
    datetime_obj = datetime.datetime.fromtimestamp(epoch)

    if not requirements["check_minutes_star"]:
        if datetime_obj.minute < requirements["check_minutes_lower"]:
            return False
        if datetime_obj.minute > requirements["check_minutes_upper"]:
            return False
    else:
        if datetime_obj.minute % requirements["check_minutes_star"] != 0:
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


def get_time_of_most_recent_check(checker_name):
    """
    Figures out the last time a check was performed for this checker,
    useful in avoiding alerts on old things we don't care about anymore.
    """
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
    return time_of_most_recent_check


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

    num_minutes_to_check = int(
        math.ceil(requirements['lookback_seconds'] / 60) * 10)
    minute_epochs_to_check = [minute_epoch_max - (60 * i) for i in range(num_minutes_to_check)]

    time_of_most_recent_check = get_time_of_most_recent_check(checker_name)
    eligible_minutes = [
        x for x in minute_epochs_to_check \
            if matches_time_pattern(requirements, x) and x > time_of_most_recent_check
    ]
    all_new_checks = []
    if eligible_minutes:
        # Refresh results, just once if we have reason to check
        rows = your_orgs_row_getter.get_rows_from_query(query_details, ())
        store_results(checker_name, rows)

        for elig_min in eligible_minutes:
            logging.info("eligible minute is %s minutes ago", ((int(time.time()) - elig_min) / 60))
            logging.info("Checking history for %s", checker_name)
            check_details = do_single_history_check(
                checker_name,
                elig_min,
                requirements
            )
            assert check_details["check_status"] in ["GOOD", "BAD"]
            all_new_checks.append(check_details)
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
    logging.info("Found %s events in the time window", event_count)
    check_status = ""

    if event_count < requirements["min_num"] or event_count > requirements["max_num"]:
        check_status = "BAD"
    else:
        check_status = "GOOD"
    assert check_status in ["GOOD", "BAD"]

    check_details = {
        "checker_name": checker_name,
        "event_count": event_count,
        "min_required": requirements["min_num"],
        "max_allowed": requirements["max_num"],
        "check_status": check_status,
        "minute_epoch": minute_epoch,
        "minute_local_time": str(datetime.datetime.fromtimestamp(minute_epoch)),
        "lookback_seconds": lookback_seconds,
    }
    return check_details


def delete_old_rows(checker_name, old_if_this_criteria):
    # This key being set to null means we don't want to do a deletion
    # One example of this is opting to do a deletion after each check with .01 probability, 
    # just to keep from doing excessive table scanning.
    if not old_if_this_criteria["delete_older_than_epoch"]:
        return False
    logging.info("Purging old rows for checker %s", checker_name)
    delete_older_than_epoch = old_if_this_criteria["delete_older_than_epoch"]
    deletion_query = """
        DELETE FROM results WHERE timestamp < ? AND checker = ?
    """
    _write_query(deletion_query, (delete_older_than_epoch, checker_name))


def get_checker_names():
    """
    Go through the config directory and figure out the checker names
    from the files there.
    """
    names = []
    files = os.listdir(CONFIGS_FOLDER)
    for fname in files:
        if fname.endswith(".dwmon"):
            names.append(fname.replace(".dwmon", ""))
    assert names, "No checker names found.  Is the configs dir empty?"
    return names


def check_all():
    """
    Get all checker names and do all their checks
    """
    checker_names = get_checker_names()
    for checker_name in checker_names:
        try:
            query_details, requirements, extra_config = parse_config_file(checker_name)
        except:
            logging.error("Couldn't parse config for checker %s", checker_name)
            raise
        for req in requirements:
            all_check_details = do_multiple_history_check(checker_name, query_details, req)
            for details in all_check_details:
                your_orgs_check_handler.handle_check(details, extra_config)
                log_check(checker_name, details["minute_epoch"])
                old_if_this_criteria = your_orgs_row_purger.identify_old(checker_name, extra_config)
                delete_old_rows(checker_name, old_if_this_criteria)


if __name__ == "__main__":
    # I put these here because if you're running the tests, you might not necessarily care
    # about testing your custom functions here - they're outside the scope of testing.
    # Mine custom handlers import some packages that others might not have.
    # I could put dummy ones into version control, but then I'd have to delete my current one.
    # We should make this more elegant - maybe make this language agnostic?
    import your_org.your_orgs_check_handler as your_orgs_check_handler
    import your_org.your_orgs_row_getter as your_orgs_row_getter
    import your_org.your_orgs_row_purger as your_orgs_row_purger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)-8s %(levelname)-8s %(message)s'
    )
    while True:
        check_all()
        logging.info("Sleeping...")
        time.sleep(60)
