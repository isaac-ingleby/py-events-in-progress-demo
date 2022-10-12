My SQLite powered Events in Progress demo
=========================================

This is designed to be used via the command line, however as I've implemented data in the repo, you
could get it to run in a number of ways pretty easily.

The calculation and corresponding output can be observed by entering at the repository root:
```bash
 python src/events_in_progress/events_in_progress.py
```

Provided no files have been tinkered with in the repository, the output should look like this:
```bash
Full number of events: 13.
Number of viable events: 10.
Minimum event start time: 2022-10-11 16:01:55.
Maximum event end time: 2022-10-12 01:01:54.

+--------------------+--------------------+--------------------+--------------------+--------------------+
|rn                  |eventStart          |eventEnd            |StartOfIntersect    |EndOfIntersect      |
+--------------------+--------------------+--------------------+--------------------+--------------------+
|0                   |2022-10-11 19:01:55 |2022-10-11 20:01:55 |2022-10-11 19:01:55 |2022-10-11 20:01:54 |
|1                   |2022-10-11 19:01:55 |2022-10-11 21:01:55 |2022-10-11 19:01:55 |2022-10-11 20:01:54 |
|2                   |2022-10-11 19:01:55 |2022-10-11 21:01:55 |2022-10-11 19:01:55 |2022-10-11 20:01:54 |
|3                   |2022-10-11 19:01:55 |2022-10-11 22:01:55 |2022-10-11 19:01:55 |2022-10-11 20:01:54 |
|4                   |2022-10-11 19:01:55 |2022-10-11 19:46:55 |2022-10-11 19:01:55 |2022-10-11 20:01:54 |
|5                   |2022-10-11 19:01:55 |2022-10-11 22:01:55 |2022-10-11 19:01:55 |2022-10-11 20:01:54 |
|6                   |2022-10-11 16:01:55 |2022-10-11 19:01:54 |2022-10-11 19:01:55 |2022-10-11 20:01:54 |
|7                   |2022-10-11 18:01:55 |2022-10-11 19:01:50 |2022-10-11 19:01:55 |2022-10-11 20:01:54 |
+--------------------+--------------------+--------------------+--------------------+--------------------+
```

This is my first time experiencing the "Events in Progress" problem, I chose a CTE approach
reminiscent of my typical way to manage the Gaps and Islands problem (which I've had to resolve
previously). I've aimed to make as much of this code into a "production-ready" state, which I feel
means less effort trying to imagine my intentions for the code.

The core of the SQLite logic is found in the `calculate_max_concurrent_events()` function:
```SQLite
    cursor.execute(
        f"""  -- this logic will find overlaps in time and eliminate all else.
      WITH events_with_date_intersects AS (
        -- first we need to calculate the beginning and end of the event intersect
        -- provided they intersect at all
    SELECT r1.rn AS rnLeft,
           r1.startTime AS startTimeLeft,
           r1.endTime AS endTimeLeft,
           r2.rn AS rnRight,
           r2.startTime AS startTimeRight,
           r2.endTime AS endTimeRight,
           -- these CASE statements would need expanding to cover more scenarios in the real
           -- world, but here they are adequate to pull out the correct dates
           CASE
             WHEN r2.startTime <= r1.endTime
             THEN r2.startTime
           END AS StartOfIntersect,
           CASE
             WHEN r1.endTime >= r2.startTime
             THEN r1.endTime
           END AS EndOfIntersect

      FROM {source_table} AS r1
      JOIN {source_table} AS r2  -- drop rows that don't intersect
        ON r2.startTime <= r1.EndTime
       AND r1.startTime <= r2.EndTime
       AND r1.rn != r2.rn  -- don't include rows that intersect with themselves
    ),
           event_with_most_date_intersects AS (
        -- Now we will find the event that most frequently intersects other events
    SELECT rnLeft,
           COUNT(rnLeft) AS frequency_count--,

      FROM events_with_date_intersects
  GROUP BY rnLeft
  ORDER BY frequency_count DESC
     LIMIT 1
    ),
           events_shared_time_period AS (
        -- Now we find the maximum time_period all events covered that intersected the
        -- most frequent event
    SELECT events.rnLeft,
           MAX(events.StartOfIntersect) AS StartOfIntersect,
           MIN(events.EndOfIntersect) AS EndOfIntersect

      FROM events_with_date_intersects AS events
      JOIN event_with_most_date_intersects AS event
        ON event.rnLeft = events.rnLeft
    )
    INSERT INTO {target_table}
        -- Insert events that intersect that along with some information on how they intersect
    SELECT ewdi.rnRight AS rn,
           ewdi.startTimeRight AS eventStart,
           ewdi.endTimeRight AS eventEnd,
           estp.StartOfIntersect,
           estp.EndOfIntersect

      FROM events_shared_time_period AS estp
      JOIN events_with_date_intersects AS ewdi
        ON ewdi.rnLeft = estp.rnLeft
       AND ewdi.StartOfIntersect <= estp.StartOfIntersect
       AND ewdi.EndOfIntersect >= estp.EndOfIntersect
    """
    )
```

As always, there's things I'd do differently a second time around, the main thing probably being
building a central class from which commands can be called with less configuration at execution.

As mentioned elsewhere, I've been slightly cheeky and used linting modules external to Python, it
keeps my code looking clean, prevents me from having to think as much about style and readability.

