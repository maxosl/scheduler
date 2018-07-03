from lib.stores.sqlite_store import SQLiteStore
from lib.models.models import Task
from lib.handlers.handler import Handler
import time
from threading import Thread
import logging

logging.basicConfig(level=logging.DEBUG, format='[%(threadName)s:%(lineno)s - %(funcName)s()] %(message)s')


class Sender(Thread):
    def __init__(self):
        super().__init__(name='SenderThread')
        self.done_tasks = []

    def send_tasks(self, tasks):
        self.done_tasks.extend(tasks)
        for task in tasks:
            print(task.data)


# setup store
store = SQLiteStore()
err = store.setup(':memory:')
if err:
    logging.error(err)

else:
    # add first task to store - executes in 5 seconds
    test_task = Task(None, time.time() + 5, 'Test', 'Task1', 0, None)
    store.put_task(test_task)
    # add second task to store - executes in 10 seconds
    test_task1 = Task(None, time.time() + 10, 'Test', 'Task2', 0, None)
    store.put_task(test_task1)

    # setup and start handler thread
    task_sender = Sender()
    handler = Handler()
    handler.setup(store, task_sender, {'limit': 1})
    handler.start()

    # wait for 6 seconds and add a task that will execute between the first and second tasks
    time.sleep(6)
    test_task2 = {'data': 'Task3', 'type': 'Test', 'now':True}
    handler.add_task(test_task2)

    # wait a bit and then close handler
    time.sleep(30)
    handler.join()
