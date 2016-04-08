Named dwmon because of original intentions - "data warehouse monitor".

# Motivation

This is designed to let you monitor counts of things in a datastore.
The foremost priority is to make it easy to add additonal checks for typical 
"business" usecases in a low/medium speed/volume environment.

I'm not sure if this is reinventing any wheels.  Even if something already exists, this is fun.  This 
is also a different way of thinking about my previous "Affirmative" project (see repo by same name).

Here is an example config for a "checker" called new_executions, in file new_executions.dwmon:

```
__QUERY__

SELECT
execution_id AS dwmon_unique_key,
ts AS dwmon_timestamp
FROM model_executions
ORDER BY dwmon_timestamp DESC
LIMIT 10000

__REQUIREMENTS__
# This is a comment
CHECKHOURS0-23 CHECKMINUTES0-59 WEEKENDS WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS180

__SOURCE__
AWS

__EXTRA__
{"some_extra_config": ["your", "arbitrary", "json"]}
```

If we have a model that runs all day, we might want to check that on
every minute of the day including weekends, when we look back in the prior 
180 seconds, we see between 5 and 20 model executions.

# Requirements

## Query result format
A "checker" is defined by creating a some_checker_name.dwmon file, in a format 
like above.

Result sets must have two colums: 1) dwmon_unique_key, and 2) dwmon_timestamp.

Results will be aggregated in another table.  When a dwmon_unique_key for the
checker has not been seen before, an entry will be put into this table with
timestamp of dwmon_timestamp.

Every minute, we check all the requirements lines for all the checkers.  
For those checkers which qualify for a check at that time, records will be 
counted by the criteria (that_time - LOOKBACKSECONDS) < dwmon_timestamp < that_time, 
and the count is checked against MINNUM and MAXNUM.

## Requirements format

You must provide 1 or more "requirements" for your checkers.

Additonal requirements can be specified on new lines in the __REQUIREMENTS__ section.

CHECKHOURS0-23 CHECKMINUTES0-59 WEEKENDS WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS180

Hours must be specified with a range like shown. To specify a single hour for a check, supply 5-5, for the 5th hour, for example.

CHECKMINUTES*/20 means "check every 20 minutes" (or when minute mod 20 == 0). This syntax is not
available for CHECKHOURS at the moment.

At least one of WEEKENDS or WEEKDAYS must be supplied.

No spaces are allowed except between options.  Numbers/ranges must not be separated 
from their options with whitespace.

# Query execution
You must define a get_rows_from_query function in your_org.your_orgs_row_getter.py.  See how this 
gets imported in dwmon.py if you are curious. This function is passed a) the query from the config, 
b) the __SOURCE__ section from the config, c) the extra JSON you defined

```
import os

import db_client

# Could do fancy stuff using query_details["souce"] if you want multiple sources
db_user = os.environ["DWMON_DB_USER"]
db_name = os.environ["DWMON_DB_NAME"]
db_host = os.environ["DWMON_DB_HOST"]

def get_rows_from_query(query_details):
    data_obj = db_client.Postgres(db=db_name, host=db_host, port=5432, user=db_user)
    results = data_obj.query(query_details["query"], ())
    return results
```

This makes things very generic.  If you know how to interpret a query so as to make a GET/POST 
request to the appropriate system instead of querying an sql database, by all means - go for it!
The function must return a list of (key, timestamp) tuples, though.

# More details about counting logic
The output of query results in the configs gets sent to a dataset like this:

```
checker_name|unique_id|timestamp
...
new_applications|123|1340000000
new_applications|789|1340000003
model_executions|123|1340000004
new_applications|456|1340000009
model_executions|101112|1340000004
...
```

Again, if a new unique_id is ever seen for checker_name, the timestamp from the
config-ed query will be added for the new row.  Counts are calculated off of
this dataset.


Every minute when checks are run, the checker rewinds a hypothetical clock 
by several multiples of LOOKBACKSECONDS.  It then advances, one minute 
at a time, until it hits the current time.  At each minute in this hypothetical 
walk, that minute is evaluated against the check's time pattern in the requirements.
If a check is eligible, an entry is made in the "checks" table with that checker name 
and the epoch corresponding to the beginning of the minute.  This means that a check 
for a minute 10 minutes in the past may be run.  This is to guard against an occasionally 
slow query from making other checks fail to run in their designated minute.  It also 
allows the system to survive a failure or temporary outage.

Before running any check, it is verified that the (hypothetical) time of the
check is after the last performed check.  This is to gaurd against situations 
where somebody changes their LOOKBACKSECONDS from 60 to 3600, and causing a 
bunch of old alerts to spring up for previous times we have already checked under 
a previous set of requirements for that checker.

# Following up on a check
You can define a handle_check function in the your_org.your_orgs_check_handler module.  This takes a 
dictionary of the fields returned by the "do_single_history_check" function in dwmon.py.  You 
can do whatever you want with that information - log it, send an email alert if bad, etc.

# Purging old rows
If you want to keep the internal counting tables lean, you can write your own logic to purge old 
rows that you don't need anymore.  Define a function (your_org.your_orgs_row_purger.identify_old) 
that returns a dict with a field "delete_older_than_epoch".  If this field is not set, nothing will happen.  
If it is set, any row for that checker older than that epoch will be purged.  I personally have my purger 
set up to draw some random numbers and just occasionally delete every older than 5 days, but you can get 
fancy however you'd like and customize each checker's retention policy maybe.

# Tricky situations / Anticipated FAQ
## My records don't have a timestamp
If you don't have a timestamp corresponding to record creation in your database, 
you can assume one yourself.  For example, you can use the current epoch time, 
or a minute or two before your query is run:

```
__QUERY__

SELECT
application_id AS dwmon_unique_key,
extract(epoch from CURRENT_TIMESTAMP) AS dwmon_timestamp
FROM applications
ORDER BY dwmon_timestamp DESC
LIMIT 10000
```

## My records don't have a unique key
You can perhaps make one:
```
__QUERY__

SELECT
customer_id || call_time AS dwmon_unique_key,
call_time AS dwmon_timestamp
FROM customer_phone_calls
ORDER BY dwmon_timestamp DESC
LIMIT 10000
```

## I want to check at several sparse hours (e.g. 9am and 1pm)
You probably have to do something like the following.  It's not the most beautiful but it keeps 
the implementation simple.

```
__REQUIREMENTS__
CHECKHOURS9-9 CHECKMINUTES0-0 WEEKENDS WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS180
CHECKHOURS13-13 CHECKMINUTES0-0 WEEKENDS WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS180
```

## The unique thing is long, like a piece of text
Maybe you can take an md5 hash or something like that.

# Testing
You can use the .dwmonsample files and rename them to .dwmon files to have them picked up by the
system.  Those refer to tables that are created in fake_records.py.  Those tables will be added 
to the sqlite database.

# Creating the db
run the create_tables function in dwmon.py

# Profiling
## Slow queries are a concern if they hold up everyone's checkers.  For that reason, some simple 
execution times are logged to help you track down the person who is causing trouble.  I haven't 
thought about how to scale this up much from what I need it for.
