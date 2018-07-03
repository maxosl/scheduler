from threading import Thread, Lock

class Sender:
    def __init__(self, task_source):
        self.task_source_lock = Lock()


    def send_tasks(self,tasks):
        for task in tasks:
            print(task)