# pylint: disable=broad-exception-caught,line-too-long,singleton-comparison,too-many-arguments,unnecessary-lambda
"""
A scheduler can can run any Python task in parallel using processes.
This file is part of pydal.
Created by Massimo Di Pierro<massimo.dipierro@gmail.com>
License: 3-clause BSD
"""

import datetime
import logging
import math
import os
import signal
import socket
import sys
import threading
import time
import traceback

from pydal import DAL, Field
from pydal.validators import IS_IN_SET

from ..utils import utcnow


def now():
    """Returns the current datetime in UTC"""
    return utcnow()


def delta(t_secs):
    """Returns a timedelta"""
    return datetime.timedelta(seconds=t_secs)


def make_daemon(func, filename, cwd="."):
    """Creates a daemon process running func in cwd and stdout->filename"""
    if os.fork():
        return
    # decouple from parent environment
    os.chdir(cwd)
    # os.setsid()
    os.umask(0)
    # do second fork
    if os.fork():
        sys.exit(0)
    # redirect standard file descriptors
    sys.__stdout__.flush()
    sys.__stderr__.flush()
    with open(os.devnull, "rb") as stream_in:
        os.dup2(stream_in.fileno(), sys.__stdin__.fileno())
        with open(filename, "wb") as stream_out:
            os.dup2(stream_out.fileno(), sys.__stdout__.fileno())
            os.dup2(stream_out.fileno(), sys.__stderr__.fileno())
            try:
                func()
            finally:
                stream_out.flush()
    sys.exit(0)


def pid_exists(pid):
    """Check For the existence of a unix pid."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def get_logger(name, log_format="[%(asctime)s]%(levelname)s %(message)s"):
    """Get a default logger"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.__stdout__)
    handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(handler)
    return logger


class Scheduler:  # pylint: disable=too-many-instance-attributes
    """Makes a scheduler"""

    __slots__ = [
        "tasks",
        "db",
        "max_concurrent_runs",
        "folder",
        "logger",
        "sleep_time",
        "worker",
        "_looping",
        "_thread",
    ]

    statuses = [
        "queued",  # the task has been queued and waiting to run
        "assigned",  # the task was assigned to a worker process
        "running",  # the task it running
        "timeout",  # the task took to long and was killed
        "completed",  # the task successfully completed
        "failed",  # the task failed
        "dead",  # the task no longer appear to be running
        "unknown",  # the task was queued but its name not in self.tasks
    ]

    def __init__(
        self,
        db,
        max_concurrent_runs=2,
        folder="/tmp/scheduler",
        sleep_time=10,
        logger=None,
    ):
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        self.db = db
        self.max_concurrent_runs = max_concurrent_runs
        self.folder = folder
        self.sleep_time = sleep_time
        self.worker = socket.gethostbyname(socket.gethostname())
        self.logger = logger or get_logger("scheduler")
        self.tasks = {}
        self._looping = False
        self._thread = None
        # make the output folder if does not exist
        os.makedirs(self.folder, exist_ok=True)
        # create a task_run table if not already provided
        if "task_run" not in db:
            db.define_table(
                "task_run",
                # given
                Field(
                    "name", requires=IS_IN_SET(lambda: self.tasks.keys()), writable=True
                ),
                Field("description", "text", writable=True),
                Field("inputs", "json", default={}, writable=True),
                Field("timeout", "integer", default=None, writable=True),
                Field("priority", "integer", default=0, writable=True),
                Field("period", "integer", default=None, writable=True),
                Field("scheduled_for", "datetime", default=now, writable=True),
                # given but reassigned
                Field(
                    "status",
                    default="queued",
                    writable=False,
                    requires=IS_IN_SET(Scheduler.statuses),
                ),
                # computed
                Field("queued_on", "datetime", default=now, writable=False),
                Field("worker", writable=False),
                Field("pid", "integer", writable=False),
                Field("started_on", "datetime", writable=False),
                Field("completed_on", "datetime", writable=False),
                Field("log", "text", writable=False),
                Field("output", "json", writable=False),
            )
            db.commit()

    def start(self):
        """Starts a thread running the main loop of the scheduler"""
        assert not self._looping and self._thread is None
        self._looping = True
        self._thread = threading.Thread(target=self.loop)
        self._thread.start()

    def stop(self):
        """Stops the main loop and joins its thread"""
        assert self._thread and self._thread is not None
        self._looping = False
        self._thread.join()
        self._thread = None

    def has_work(self):
        """Checks whether the scheduler has any work queued, assigned, or running"""
        db = self.db
        return (
            db(db.task_run.status.belongs(("queued", "assigned", "running"))).count()
            > 0
        )

    def loop(self):
        """Runs the main loop of the scheduler"""
        db = self.db
        self.logger.info("worker %s/%s start", self.worker, id(self))
        while self._looping:
            self.logger.info("loop start (tasks in db %i)", db(db.task_run).count())
            if not self.step():
                time.sleep(self.sleep_time)
            self.logger.info("end loop")
        self.logger.info("worker %s/%s stop", self.worker, id(self))

    def step(self):
        """Runs one step of the scheduler"""
        db = self.db
        # find corrupted runs and re-enqueue them
        wruns = db(db.task_run.worker == self.worker)
        wruns(db.task_run.status == "assigned").update(status="queued")
        db.commit()
        # check on task timeout
        runs = wruns(db.task_run.status == "running").select()
        num_running = 0
        t_end = now()
        for run in runs:
            # check for processes that died
            if not pid_exists(run.pid):
                log = self.retrieve_log(run) + self._end_tag(
                    status="dead", completed_on=t_end
                )
                run.update_record(status="dead", log=log, completed_on=t_end)
                db.commit()
                self.logger.info("run died #%i %s", run.id, run.name)
            # check for processes that timedout
            if run.timeout and run.started_on + delta(run.timeout) < now():
                os.kill(run.pid, signal.SIGKILL)
                log = self.retrieve_log(run) + self._end_tag(
                    status="timeout", completed_on=t_end
                )
                run.update_record(status="timeout", log=log, completed_on=t_end)
                db.commit()
                self.logger.info("run timeout #%i %s", run.id, run.name)
            else:
                num_running += 1

        # if too main runs wait before retrying
        if num_running >= self.max_concurrent_runs:
            self.logger.info("too many running tasks")
            return False

        # try find a new run else wait before retrying
        run = self.next_run()
        if not run:
            self.logger.info("no new tasks")
            return False

        # if the next is unknown, skip it
        self.logger.info("new run: %s", run.name)
        if run.name not in self.tasks:
            run.update_record(status="unknown")
            db.commit()
            return True

        # make a child and assign it the run
        filename = self.get_output_filename(run)
        try:
            make_daemon(lambda run=run: self.safe_exec_child(run), filename)
        except OSError as err:
            self.logger.error("Fork error: %s", err)
        except Exception:
            self.logger.error(traceback.format_exc())
        return True

    def next_run(self):
        """Returns the next run ready to be executed"""
        db = self.db
        # find the next task to execute
        nruns = db(db.task_run.worker == None)(db.task_run.scheduled_for <= now())
        orderby = db.task_run.priority | db.task_run.id
        run = nruns.select(orderby=orderby, limitby=(0, 1)).first()
        self.logger.info("run found #%s %s", run and run.id, run and run.name)
        if run:
            # try assign the run to self if no other process stole it
            try:
                if (
                    nruns(db.task_run.id == run.id).update(
                        worker=self.worker, status="assigned"
                    )
                    > 0
                ):
                    db.commit()
                    self.logger.info("run assigned #%i %s", run.id, run.name)
                    return run
            except Exception:
                # some other process stole it, rollback and loop to try again
                self.logger.warning(traceback.format_exc())
                db.rollback()
        # run not fund
        return None

    def safe_exec_child(self, run):
        """Run exec_child in a try check to reconnect/commit/rollback all databases"""
        try:
            self.db.get_connection_from_pool_or_new()
            self.exec_child(run)
            self.db.recycle_connection_in_pool_or_close("commit")
        except Exception:
            self.db.recycle_connection_in_pool_or_close("rollback")

    def exec_child(self, run):
        """Executes the run, to be called by the child process, any output here goes into log file"""
        db = self.db
        pid = os.getpid()
        started_on = now()
        # print the header in the output log file
        print(self._begin_tag(run, started_on))
        # update the record
        run.update_record(status="running", started_on=started_on, pid=pid)
        db.commit()
        self.logger.info("run started #%i %s", run.id, run.name)
        # run the task safely
        try:
            output = self.tasks[run.name](**run.inputs)
            status, tb = "completed", None
        except Exception:
            status, output, tb = "failed", None, traceback.format_exc()
        # record the completion
        completed_on = now()
        # retrieve the log and update the current task
        log = self.retrieve_log(run) + self._end_tag(status, completed_on, tb)
        run.update_record(
            status=status, output=output, log=log, completed_on=completed_on
        )
        db.commit()
        self.logger.info("run %s #%i %s", status, run.id, run.name)
        # if periodic task, reschedule it (mind errors below will not be logged)
        if run.period:
            wait_time = (run.completed_on - run.scheduled_for).total_seconds()
            dt = run.period * math.ceil(wait_time / run.period)
            scheduled_for = run.scheduled_for + delta(dt)
            self.enqueue_run(
                run.name,
                run.description,
                run.inputs,
                run.timeout,
                run.priority,
                run.period,
                scheduled_for,
            )

    def _begin_tag(self, run, started_on):
        """Generate the begin <run> tag for the output file"""
        return f'<run id="{run.id}" worker="{run.worker}" pid="{run.pid}" started_on="{started_on}">'

    def _end_tag(self, status, completed_on, tb=None):
        """Generate the end </run> tag for the output file"""
        msg = f"\n<traceback>\n{tb.strip()}\n</traceback>" if tb else ""
        return f'{msg}\n</run status="{status}" completed_on="{completed_on}">'

    def retrieve_log(self, run):
        """Retrieve the log for the run"""
        try:
            filename = self.get_output_filename(run)
            with open(filename, "rb") as stream:
                log = stream.read().decode("utf8", errors="ignore")
            os.unlink(filename)
        except Exception as err:
            log = f'<missing reason="{err}"/>'
        return log.strip()

    def get_output_filename(self, run):
        """Generate the fullname for the output log file"""
        return os.path.join(self.folder, f"{run.id}.txt")

    def register_task(self, task_name, function):
        """Register a new task, given a name and a function"""
        self.tasks[task_name] = function

    def enqueue_run(
        self,
        name,
        description="",
        inputs={},
        timeout=3600,
        priority=0,
        period=None,
        scheduled_for=None,
    ):
        """Stores a new db.task_run"""
        db = self.db
        assert name in set(dict(db.task_run.name.requires.options()))
        assert inputs is None or isinstance(inputs, dict)
        t = now()
        run_id = db.task_run.insert(
            name=name,
            description=description,
            inputs=inputs or {},
            timeout=timeout,
            priority=priority,
            period=period,
            scheduled_for=scheduled_for or t,
            queued_on=t,
            status="queued",
        )
        db.commit()
        self.logger.info("run enqueued %s", run_id)


def run_example():
    """Example of usage"""

    def mytask():
        print(f"hello world {datetime.datetime.now()}")
        return "Done!"

    db = DAL("sqlite://storage.sqlite")
    scheduler = Scheduler(db)
    db(db.task_run).delete()
    # a task that says hello
    scheduler.register_task("hello", mytask)
    scheduler.register_task("periodic", mytask)
    # a task that fails if x is 0
    scheduler.register_task("fail", lambda x: 1 / x)
    # task that takes 10 seconds
    scheduler.register_task("long", lambda: time.sleep(10))
    # enqueue a run for the fail task with input x=0 (for indeed fails)
    scheduler.enqueue_run(name="fail", inputs={"x": 0})
    # a run that takes 10s but with timeout of 1s so timeouts
    scheduler.enqueue_run(name="long", timeout=1)
    # a run scheduled to start in 20s
    scheduler.enqueue_run(name="hello", scheduled_for=now() + delta(20))
    # a regular run to start asap
    scheduler.enqueue_run(name="hello")
    # a run that should rerun every 10s
    scheduler.enqueue_run(name="periodic", period=10)
    db.commit()
    scheduler.start()
    while scheduler.has_work():
        # do not all more than 3 periodic tasks
        if db(db.task_run.name == "periodic").count() > 3:
            db(db.task_run.name == "periodic")(db.task_run.status == "queued").delete()
            db.commit()
        time.sleep(1)
    scheduler.stop()


if __name__ == "__main__":
    run_example()
