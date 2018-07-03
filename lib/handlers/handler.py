from lib.models.models import Task, UpdateTask
from lib.schedulers.scheduler import Scheduler
from threading import Lock
from lib.parsers import JsonParser
from threading import Thread
import logging

class Handler(Thread):
    def __init__(self):
        super().__init__(name="HandlerThread")
        self._scheduler_lock = Lock()
        self._store_lock = Lock()
        self._store = None
        self._scheduler = None
        self._task_sender = None

    '''
        Setup the scheduler and run scheduler and sender threads. Also, get the store.
    '''
    def setup(self, store, task_sender, scheduler_config):
        logging.info('Setting up handler')
        self._store = store

        self._task_sender = task_sender
        self._task_sender.start()

        self._scheduler = Scheduler(store, self, scheduler_config)
        self._scheduler.start()

    '''
        Add a new task
    '''
    def add_task(self, task_json):
        logging.debug('New task! parse it and add it.')
        task = JsonParser.parse_task_from_json(task_json)
        with self._scheduler_lock:
            self._scheduler.put_task(task)

        with self._store_lock:
            self._store.put_task(task)

    '''
        For all scheduled jobs that are done waiting - update their status to done if they do not repeat,
        else set their next execution time.
        Send all tasks to the task sender.
    '''
    def process_done_tasks(self, tasks):
        logging.debug('Processing due tasks')
        update_tasks = []
        for task in tasks:
            update_task = UpdateTask(task.id)
            if task.repeat_type:
                execution_time = JsonParser.get_execution_time_repeated(task.repeat_type, task.repeat)
                update_task.execution_time = execution_time

            update_tasks.append(update_task)

        with self._store_lock:
            self._store.update_done_tasks(update_tasks)

        self._task_sender.send_tasks(tasks)

    def join(self, timeout=None):
        self._scheduler.stop()
        self._task_sender.join()
        super().join()

