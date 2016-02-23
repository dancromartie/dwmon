import argparse
import sqlite3
import time

import config

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", type=int, help="lookback from now, in seconds")
    parser.add_argument("-c", help="checker name")
    args = parser.parse_args()

    db_conn = sqlite3.connect(config.SQLITE_DB_NAME)

    lookback_seconds = args.s
    checker = args.c

    epoch_lower = time.time() - lookback_seconds

    query = """
        SELECT
        strftime('%w', datetime(timestamp, 'unixepoch')) as day_of_week,
        strftime('%H', datetime(timestamp, 'unixepoch')) as hour,
        min(datetime(timestamp, 'unixepoch', 'localtime')) as mytime,
        count(1)
        FROM
        results
        WHERE day_of_week IN ('1', '2', '3', '4', '5')
        AND checker = ?
        AND timestamp > ?
        GROUP BY hour, day_of_week
        ORDER BY day_of_week, hour ASC
    """

    print "day_of_week|hour|min_local_time|count"
    results = db_conn.cursor().execute(query, (checker, epoch_lower))
    for result in results:
        print "%s|%s|%s|%s" % (result[0], result[1], result[2], result[3])
