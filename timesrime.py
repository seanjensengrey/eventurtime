import arrow
import os
import queue
import sqlite3
import subprocess
import time
import toml
import threading

import pdb

import collections

import httpd_server

ProcessEvent = collections.namedtuple("EventRecord", "event_type start_ts end_ts command output return_code")
Job = collections.namedtuple("Job", "time_delta_s command")

def init_db(database_path):
    if not os.path.isfile(database_path):
        conn = sqlite3.connect(database_path)
        create_schema(conn)
        insert_jobs(conn)
        return conn
    else:
        conn = sqlite3.connect(database_path)
        return conn

def create_schema(conn):
        conn.execute("""\
create table process_event (
    start_ts        integer,
    end_ts          integer,
    command         text,
    output          text,
    return_code     integer
)""")
        conn.commit()
        conn.execute("""
create table job (
    time_delta_s    integer,
    active          integer,
    command         text
)
""")
        conn.commit()
        conn.execute("""
create table http_event (
    timestamp       integer,
    command         text,
    headers         text,
    url             text,
    client_ip       text,
    body            text
)
""")
        conn.commit()


def insert_jobs(conn):
    conn.execute("insert into job(time_delta_s, active, command) values (?,?,?)", (15,1,"/Users/basho/x.env/bin/riak-mesos node status"))
    conn.execute("insert into job(time_delta_s, active, command) values (?,?,?)", (15,1,"/Users/basho/x.env/bin/riak-mesos node transfers --node riak-default-1"))
    conn.commit()


def insert_debug_jobs(conn):
    conn.execute("insert into job(time_delta_s,active, command) values (?,?,?)", (30,1,"date"))
    conn.commit()


def insert_proc_event(conn, event):
    e = event
    assert isinstance(e, ProcessEvent)
    conn.execute("insert into process_event values (?,?,?,?,?)", (e.start_ts, e.end_ts, e.command, e.output, e.return_code))
    conn.commit()


def insert_http_event(conn, event):
    e = event
    assert isinstance(e, httpd_server.HttpEvent)
    conn.execute("insert into http_event values (?,?,?,?,?,?)", (e.timestamp, e.command, e.headers, e.url, e.client_ip, e.body))
    conn.commit()


def run(job,at_timestamp,q):
    now = time.time()
    delta_t = at_timestamp - now

    def job_thread():
        if delta_t > 0:
            print("job:{} sleeping_for:{}".format(job.command, delta_t))
            time.sleep(delta_t)

        start_ts = arrow.utcnow()
        proc = subprocess.run(job.command.split(' '), stdout=subprocess.PIPE)
        end_ts = arrow.utcnow()
        event = ProcessEvent('PROC', start_ts.timestamp, end_ts.timestamp, job.command, proc.stdout.strip(), proc.returncode)
        q.put(event)

    t = threading.Thread(target=job_thread)
    t.start()
    return t

def handle_event(c, event):
    if event.event_type == 'PROC':
        insert_proc_event(c, event)

    if event.event_type == 'HTTP':
        if event.url == '/favicon.ico':
            pass
        else:
            insert_http_event(c, event)


def http_main(c, port):
    print("starting http on {}".format(port))
    event_queue = httpd_server.mk_http_queue(port)

    def job_queue(jobs):
        now = time.time

        # stores last timestamp that a job was *scheduled* to run
        last_job_t = collections.defaultdict(lambda: 0)

        while 1:
            job_threads = []
            for job in jobs:
                try:
                    last_t = last_job_t[job.command]
                    last_job_t[job.command] = now()
                    job_threads.append(run(job, max(last_t + job.time_delta_s, last_job_t[job.command]), event_queue))
                except Exception as e:
                    print(e)
                    print(job, "failed")

            print(job_threads)
            for t in job_threads:
                t.join()

    ## BROKEN, we can only access sqlite from the main thread
    # pull fresh jobs for each invocation
    # allows submitting new jobs during execution
    # this won't pickup new jobs until the longest job has completed

    def mk_job(*args):
        return Job(int(args[0]),args[1])


    jobs = [Job(*result) for result in c.execute("SELECT time_delta_s,command FROM job WHERE active = 1")]
    job_manager = threading.Thread(target=job_queue, args=(jobs,))
    job_manager.daemon
    job_manager.start()


    while 1:
        try:
            event = event_queue.get(block=True)
            print("EVNTQ",event)

            if event is not None:
                handle_event(c, event)
                event = None
        except Exception as e:
            print(e)
            time.sleep(1)





def test_http():
    c = sqlite3.connect("test-http-server.db")
    create_schema(c)
    insert_debug_jobs(c)
    http_main(c, 9003)


if __name__ == "__main__":
    # c = init_db("biggums-test.db")
    # main(c)
    test_http()