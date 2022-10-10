# -*- coding: utf-8 -*-
"""The main logic for the events in progress demo using exclusively Python, although additional
libraries have been utilised to speed up linting.

Because the brief says not to utilise anything like pandas, I've instead opted for stringing
together a group of dataclasses that will contain an index (to stop me from going mad).

This relies on the principle that this VideoPlayEvent is provided to us in a structured manner,
i.e. that the file looks artificially reminiscent of a .json file, as like a json it uses curly
brackets `{}` but unlike a json it does not use the delimiters that would normally be expected with
a json: commas. It instead relies on new lines. With this assumption in place we can begin.

Example file: events01.txt
```
# this event is one hour in length
VideoPlay {
startTime : 1665425095.589787
endTime : 1665428695.589787
}
```

This program also assumes that there is some method either of watermarking, or of clearing out
older, already processed files.
"""

from dataclasses import dataclass
from pathlib import Path
from re import search as re_search
from typing import List

import sys


@dataclass
class VideoPlayEvent:
    """A simple dataclass object from which we can pull attributes."""
    index: int
    start_time: float  # unix timestamp
    end_time: float  # unix timestamp


def get_timestamp(
    timestamp_type: str, event_line: str, event_file: Path, event_line_count: int
) -> float:
    """Extract a timestamp from a file line using regular expressions.

    Args:
        timestamp_type: the timestamp pattern of interest
        event_line: the line of the file currently being read.
        event_file: the file being read, for diagnostic purposes only.
        event_line_count: the line of the file being read, for diagnostic purposes only.

    Returns:
        The start-time as a unix-timestamp.

    Raises:
        AttributeError if no value matching the pattern can be found.
    """

    try:
        start_time = re_search(f"{timestamp_type}:(.*)", event_line).group(1)
    except AttributeError:
        raise AttributeError(
            f"A line is malformed on line {event_line_count} of '{event_file}', expected "
            f"timestamp float to be presented after {timestamp_type}.\nFull line: {event_line}"
        )

    return float(start_time)


def generate_data_group(event_path: str) -> List[VideoPlayEvent]:
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

    for file_count, event_file in enumerate(Path(event_path).glob("*")):
        # print(event_file.read_text())
        line_count = 0
        for event_line in event_file.read_text().replace(" ", "").splitlines():
            if event_line.startswith("#"):
                # this section would never need to exist in production code, but it's useful in
                # case you want to explore how I've laid out the json-like files.
                continue

            if event_line.endswith("{"):
                open_event = True  # new event has been initiated
                start_time, end_time = None, None  # reset to none
                print(event_line, line_count, event_count)  # temporary

            # extract the timestamp from the string
            elif event_line.startswith("startTime") and open_event:
                start_time = get_timestamp("startTime", event_line, event_file, line_count)
            elif event_line.startswith("endTime") and open_event:
                end_time = get_timestamp("endTime", event_line, event_file, line_count)
            elif event_line.endswith("}") and open_event and start_time and end_time:
                # only triggered if both start_time and end_time are not None
                data_group.append(VideoPlayEvent(event_count, start_time, end_time))
                event_count += 1
                open_event = False  # event has closed

            elif event_line.endswith("}"):
                # unhappy path, do not append events
                open_event = False
                start_time, end_time = None, None  # set to none so they can be filtered out later

            line_count += 1

    return data_group


if __name__ == "__main__":
    data = generate_data_group(sys.argv[1])

    # some very basic data analysis for now, more complex logic to follow
    # (and eventually be moved to their own functions)
    minimum_start_time = min([event.start_time for event in data])
    maximum_end_time = max([event.end_time for event in data])

    print(data, f"minimum_start_time={minimum_start_time}, maximum_end_time={maximum_end_time}.")
