# -*- coding: utf-8 -*-
"""The main logic for the events in progress demo using exclusively Python, although additional
libraries have been utilised to speed up linting.

Because the brief says not to utilise anything like pandas, I've instead opted for a blend of
dataclasses and SQLite tables which will together reference an event.

This relies on the principle that this VideoPlayEvent is provided to us in a structured manner,
i.e. that the file looks artificially reminiscent of a .json file, as like a json it uses curly
brackets `{}` but unlike a json it does not use the delimiters that would normally be expected with
a json: commas. It instead relies on new lines. With this assumption in place we can begin.

Example file: events01.txt
```
# this event is one hour in length
VideoPlay {
startTime : 2022-10-11 19:01:55
endTime : 2022-10-11 20:01:55
}
```

This program also assumes that there is some method either of watermarking, or of clearing out
older, already processed files.
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from re import search as re_search
from sqlite3 import Connection
from typing import List, Tuple


@dataclass
class VideoPlayEvent:
    """A dataclass object from which we can pull attributes."""

    index: int
    start_time: float  # unix-timestamp
    end_time: float  # unix-timestamp

    def __post_init__(self):
        """SQLite has limited support for datetime, so I'm adding some functionality to manage it
        via the class. This was mainly used in debugging stage
        """
        self.start_dtime = datetime.fromtimestamp(self.start_time)
        self.end_dtime = datetime.fromtimestamp(self.end_time)
        self.duration: timedelta = self.end_dtime - self.start_dtime
        # flag events that fall outside expected behaviour on the events themselves
        self.is_3_hours_or_less: bool = self.duration <= timedelta(hours=3)
        self.start_proceeds_end: bool = self.start_time < self.end_time

    def return_core_attributes(self) -> Tuple[int, float, float]:
        """Returns core attributes for clearer formatting."""

        return self.index, self.start_time, self.end_time


def get_dt_from_tstamp(timestamp: float) -> datetime:
    """Get the datetime value from a unix-timestamp float.

    Returns:
        Datetime value.
    """

    return datetime.fromtimestamp(timestamp)


def get_timestamp_from_events(
    timestamp_type: str, event_line: str, event_file: Path, event_line_count: int
) -> float:
    """Extract a datetime timestamp from a file line using regular expressions.

    Args:
        timestamp_type: the timestamp pattern of interest
        event_line: the line of the file currently being read.
        event_file: the file being read, for diagnostic purposes only.
        event_line_count: the line of the file being read, for diagnostic purposes only.

    Returns:
        The start-time as a unix-timestamp float.

    Raises:
        AttributeError if no value matching the pattern can be found.
    """

    try:
        start_time = re_search(f"{timestamp_type}:(.*)", event_line).group(1)
    except AttributeError as err:
        raise AttributeError(
            f"A line is malformed on line {event_line_count} of '{event_file}', expected "
            f"timestamp float to be presented after {timestamp_type}.\nFull line: {event_line}"
        ) from err

    # sqlite has difficulty with complex datatypes converting to unix timestamp
    dt_object = datetime.strptime(start_time, "%Y-%m-%d%H:%M:%S")
    return datetime.timestamp(dt_object)


def generate_event_group(event_path: Path) -> List[VideoPlayEvent]:
    """The intention here is to have a list of class instances which we can interrogate, it's my
    attempt to get as close as possible to the concept of a Spark/Pandas Dataframe in a quick
    fashion.

    Args:
        event_path: the path where the events are stored

    Returns:
        A tuple of start-time and end-time timestamps as floats
    """

    # "If I had more time, I would have written a shorter letter"

    data_group = []  # intended as a collection of VideoPlayEvent instances
    #  open_event flag ensures events are only recorded when they begin properly with a '{'
    open_event = False
    event_count = 0  # there may be multiple events per file, keep track of

    for event_file in event_path.glob("*"):

        line_count = 0
        for event_line in event_file.read_text().replace(" ", "").splitlines():
            if event_line.startswith("#"):
                # this is here so that I can interpose comments in the events files.
                continue

            if event_line.endswith("{"):
                open_event = True  # new event has been initiated
                start_time, end_time = None, None  # reset to none

            # extract the timestamp from the string
            elif event_line.startswith("startTime") and open_event:
                start_time = get_timestamp_from_events(
                    "startTime", event_line, event_file, line_count
                )
            elif event_line.startswith("endTime") and open_event:
                end_time = get_timestamp_from_events(
                    "endTime", event_line, event_file, line_count
                )
            elif event_line.endswith("}") and open_event and start_time and end_time:
                # only triggered if start_time and end_time are not None
                data_group.append(VideoPlayEvent(event_count, start_time, end_time))
                event_count += 1
                open_event = False  # event has closed

            elif event_line.endswith("}"):
                # unhappy path, do not append events
                open_event = False
                start_time, end_time = None, None  # set to none so they can be filtered out later

            line_count += 1

    return data_group


def get_table_results(connection: Connection, table: str) -> str:
    """Return the results of the table as a string. Meant to somewhat mimic the behaviour of Sparks
    `Dataframe.show()`.

    Args:
        connection: The SQLite connection with which the function will query the result.
        table: The table you wish to query.

    Returns:
        The query result as a string.
    """
    cursor = connection.cursor()

    # return results of query
    cursor.execute(f"SELECT * FROM {table}")
    query_result = cursor.fetchall()

    # extract column names for formatting from table schema info
    cursor.execute("PRAGMA table_info(events_max_simultaneous)")
    columns_names = [column_info[1] for column_info in cursor.fetchall()]

    formatted_result = ""  # add to this string

    # define the borders
    column_title = f"{'-' * 20}+"
    border_string = f"+{column_title * 5}"
    formatted_dt_col_names = [f"{col}".ljust(20) for col in columns_names]
    column_name_str = f"|{'|'.join(formatted_dt_col_names)}|"
    formatted_result += f"{border_string}\n{column_name_str}\n{border_string}\n"

    # define how the table contents are presented
    for row in query_result:
        formatted_result += (  # attempt to prettify the output somewhat
            f"|{str(row[0]).ljust(20)}|{get_dt_from_tstamp(row[1])} "
            f"|{get_dt_from_tstamp(row[2])} |{get_dt_from_tstamp(row[3])} "
            f"|{get_dt_from_tstamp(row[4])} |\n"
        )
    formatted_result += border_string

    return formatted_result


def run():
    """Executes the main logic and outputs information to terminal via print function."""

    directory_root = Path(__file__).parent.parent.parent

    events = generate_event_group(directory_root / "tests" / "data")

    cleaned_events = [  # eliminate events that don't make much sense (I've interpreted "few" as 3)
        event for event in events if event.is_3_hours_or_less and event.start_proceeds_end
    ]

    # some very basic data analysis for now, more complex logic to follow
    # (and eventually be moved to their own functions)
    min_start_time = min(event.start_time for event in cleaned_events)
    max_end_time = max(event.end_time for event in cleaned_events)

    # report output
    print(
        f"Full number of events: {len(events)}.\nNumber of viable events: {len(cleaned_events)}.\n"
        f"Minimum event start time: {get_dt_from_tstamp(min_start_time)}.\n"
        f"Maximum event end time: {get_dt_from_tstamp(max_end_time)}.\n"
    )

    with sqlite3.connect(directory_root / "events.db") as conn:  # connect to sqlite3 database
        cursor = conn.cursor()  # create a cursor

        cursor.execute("DROP TABLE IF EXISTS events_raw")
        cursor.execute(
            """
        CREATE TABLE events_raw (
            rn INTEGER,
            StartTime INTEGER,
            EndTime INTEGER          
        )
        """
        )

        cursor.execute("DROP TABLE IF EXISTS events_max_simultaneous")
        cursor.execute(
            """
        CREATE TABLE events_max_simultaneous (
            rn INTEGER,
            eventStart INTEGER,
            eventEnd INTEGER,
            StartOfIntersect INTEGER,
            EndOfIntersect INTEGER
        )
        """
        )

        cursor.executemany(  # insert all these events into the events.events table
            f" INSERT INTO events_raw VALUES (?,?,?)",
            [event.return_core_attributes() for event in cleaned_events]
        )

        cursor.execute(  # this logic will find overlaps in time and eliminate all else.
            """
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

          FROM events_raw AS r1
          JOIN events_raw AS r2  -- drop rows that don't intersect
            ON r2.startTime <= r1.EndTime
           AND r1.startTime <= r2.EndTime
           AND r1.rn != r2.rn  -- don't include rows that intersect with themself 
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
        INSERT INTO events_max_simultaneous
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
        conn.commit()

        table_result = get_table_results(connection=conn, table="events_max_simultaneous")
        print(table_result)


if __name__ == "__main__":
    run()
