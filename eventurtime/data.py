import collections

Job = collections.namedtuple("Job", "time_delta_s command")

HttpEvent = collections.namedtuple("HttpEvent", "event_type timestamp command headers url client_ip body")
ProcessEvent = collections.namedtuple("EventRecord", "event_type start_ts end_ts command output return_code")


