#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Unit tests for scheduler.py"""

import datetime
import tempfile
import time
import unittest

from pydal import DAL
from pydal.tools.scheduler import Scheduler, delta, now


class TestScheduler(unittest.TestCase):
    def test_scheduler(self):
        completed_tasks = []

        def mytask():
            print(f"hello world {datetime.datetime.now()}")
            return "Done!"

        with tempfile.TemporaryDirectory() as tempdir:
            db = DAL("sqlite://store.sqllite", folder=tempdir)
            scheduler = Scheduler(db, sleep_time=1, max_concurrent_runs=1)
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
            scheduler.enqueue_run(name="hello", scheduled_for=now() + delta(2))
            # a regular run to start asap
            scheduler.enqueue_run(name="hello")
            # a run that should rerun every 10s
            scheduler.enqueue_run(name="periodic", period=2)
            db.commit()
            scheduler.start()
            while True:
                # do not all more than 3 periodic tasks
                if db(db.task_run.name == "periodic").count() > 3:
                    db(db.task_run.name == "periodic")(
                        db.task_run.status == "queued"
                    ).delete()
                    db.commit()
                    break
                time.sleep(0.1)
            scheduler.stop()
            self.assertEqual(db(db.task_run).count(), 7)
            self.assertEqual(db(db.task_run.status == "queued").count(), 0)
            self.assertEqual(db(db.task_run.status == "timeout").count(), 1)
            self.assertEqual(db(db.task_run.status == "completed").count(), 5)
            self.assertEqual(db(db.task_run.status == "failed").count(), 1)
            # for run in db(db.task_run).select():
            #     print(run.name, run.status, run.log)
