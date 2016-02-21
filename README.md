# Motivation

This is designed to let you monitor counts of things in a data warehouse.  
The foremost priority is to make it easy to add additonal checks for typical 
"business" usecases in a low/medium speed/volume environment.

There is no real need to have this tied to datawarehouse use cases, but something 
entirely general and applicable to generic batch/laggy/realtime/high-volume use 
cases complicates the design considerably.

Here is an example config:

```
__QUERY__

SELECT
execution_id AS dwmon_unique_key,
ts AS dwmon_timestamp
FROM model_executions
ORDER BY ts DESC
LIMIT 10000

__REQUIREMENTS__
CHECKHOURS0-23 CHECKMINUTES0-59 WEEKENDS WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS180
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

Every minute, a daemon checks all the requirements lines for all the checkers.  
For those checkers which qualify for a check at that time, records will be 
counted by the criteria (now - LOOKBACKSECONDS) < dwmon_timestamp < now, 
and the count is checked against MINNUM and MAXNUM.

## Requirements format
You may provide 1 or more "requirements" for your checkers.
Additonal requirements can be specified on new lines in the __REQUIREMENTS__ section.

CHECKHOURS0-23 CHECKMINUTES0-59 WEEKENDS WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS180

All CHECKHOURS and CHECKMINUTES must be specified with a range.  To specify 
a single minute when to check things, supply 5-5, for the 5th minute, for example.

At least one of WEEKENDS or WEEKDAYS must be supplied.

No spaces are allowed except between options.  Numbers/ranges must not be separated 
from their options with whitespace.

# More details about counting logic
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

# Alerting
No support for alerts is currently available.  It's suggested that you 
scan the output of this system's print statements to figure out when you should 
alert.

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
ORDER BY ts DESC
LIMIT 10000
```

## I want to check several times within an hour (e.g. 0th minute and 30th minute)
Try something like the following.  It's not the most beautiful but it keeps 
the implementation simple.

```
__REQUIREMENTS__
CHECKHOURS0-23 CHECKMINUTES0-0 WEEKENDS WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS180
CHECKHOURS0-23 CHECKMINUTES30-30 WEEKENDS WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS180
```


