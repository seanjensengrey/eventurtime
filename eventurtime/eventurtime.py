import arrow
import os
import queue
import sqlite3
import subprocess
import threading
import time

import collections

from . import httpd_server
from .data import Job, ProcessEvent, HttpEvent

def init_db(database_path):
    if not os.path.isfile(database_path):
        conn = sqlite3.connect(database_path)
        create_schema(conn)
        init_jobs(conn)
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

def init_jobs(conn):
    insert_job(conn, Job(time_delta_s=60, command='date'))


def insert_job(conn, job):
    conn.execute("insert into job(time_delta_s, active, command) values (?,?,?)",
                 (job.time_delta_s, 1, job.command))
    conn.commit()


def insert_proc_event(conn, event):
    e = event
    assert isinstance(e, ProcessEvent)
    conn.execute("insert into process_event values (?,?,?,?,?)",
                 (e.start_ts, e.end_ts, e.command, e.output, e.return_code))
    conn.commit()


def insert_http_event(conn, event):
    e = event
    assert isinstance(e, httpd_server.HttpEvent)
    conn.execute("insert into http_event values (?,?,?,?,?,?)",
                 (e.timestamp, e.command, e.headers, e.url, e.client_ip, e.body))
    conn.commit()


def dktr_minperiod(seconds=1):
    def dktr(wrapped_func):
        last_call = [ 0, None ]
        def fn(*args,**kwargs):
            if time.time() - last_call[0] > seconds:
                result = wrapped_func(*args,**kwargs)
                last_call[0] = time.time()
                last_call[1] = result
                return result
            else:
                return last_call[1]
        return fn
    return dktr



def run(job,at_timestamp,q):
    now = time.time()
    delta_t = at_timestamp - now

    def job_thread():
        if delta_t > 0:
            print("job:{} sleeping_for:{}".format(job.command, delta_t))
            time.sleep(delta_t)

        start_ts = arrow.utcnow()
        proc = subprocess.Popen(job.command.split(' '), stdout=subprocess.PIPE)
        stdout = proc.communicate()[0]
        end_ts = arrow.utcnow()
        event = ProcessEvent('PROC', start_ts.timestamp, end_ts.timestamp, job.command, stdout.strip(), proc.returncode)
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

    def job_manager(update_q):
        now = time.time

        # stores last timestamp that a job was *scheduled* to run
        last_job_t = collections.defaultdict(lambda: 0)

        jobs = []
        while 1:
            try:
                new_jobs = update_q.get(block=True,timeout=1)
            except queue.Empty as e:
                pass
            else:
                if new_jobs is not None:
                    jobs = new_jobs

            job_threads = []
            for job in jobs:
                try:
                    last_t = last_job_t[job.command]
                    future_t = max(last_t + job.time_delta_s, now())
                    last_job_t[job.command] = future_t

                    job_threads.append(run(job, future_t, event_queue))
                except Exception as e:
                    print(e)

            print(job_threads)
            for t in job_threads:
                t.join()


    job_update_q = queue.Queue()

    job_daemon = threading.Thread(target=job_manager, args=(job_update_q,))
    job_daemon.daemon
    job_daemon.start()

    @dktr_minperiod(seconds=15)
    def enqueue_new_jobs():
        jobs = [Job(*result) for result in c.execute("SELECT time_delta_s,command FROM job WHERE active = 1")]
        job_update_q.put(jobs)


    while 1:
        try:
            enqueue_new_jobs()
            event = event_queue.get(block=True)
            print("EVNTQ",event)

            if event is not None:
                handle_event(c, event)
                event = None

        except Exception as e:
            print(e)
            time.sleep(1)



def main(database, port):
    """
    python eventuretime.py --database perf-run.db 9005

    then in another
    """
    c = init_db(database)
    http_main(c, port)