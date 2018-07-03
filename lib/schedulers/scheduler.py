from threading import Thread, Event, Condition
import logging
import time


class Scheduler(Thread):
    def __init__(self, store, handler, config):
        super().__init__(name='SchedulerThread')
        self._conditionObj = Condition()
        self._store = store
        self._handler = handler
        self._stop_event = Event()
        self._new_task = None
        self.last_exec_time = time.time()

        self._config = config

    def run(self):
        logging.info('Started scheduler')
        limit = self._config['limit']

        # Run the main scheduler loop while stop() has not been called.
        while not self._stop_event.is_set():
            # Check if a new task with higher priority was received from the handler
            if self._new_task:
                # Schedule new higher priority task
                logging.debug('Got new task from handler')
                task = self._new_task
                self._new_task = None
                self.__schedule_tasks([task])

            # else check if due tasks are available in the store
            else:
                logging.debug('Got new task from storage ')
                ret = self._store.get_next_tasks(self.last_exec_time, limit)
                if ret['error']:
                    logging.error(ret['error'])
                    return

                else:
                    # if due tasks are available schedule them
                    if ret['data'] and len(ret['data']) > 0:
                        self.__schedule_tasks(ret['data'])
                    else:
                        # if no due tasks are available, wait until notified of new task
                        # by handler
                        with self._conditionObj:
                            self._conditionObj.wait()

    ''' 
        Schedule a new task and send it to the task sender when time is up
    '''
    def __schedule_tasks(self, tasks):
        logging.debug('Scheduling new tasks')
        self.last_exec_time = tasks[0].execution_time
        logging.debug('Tasks will be executed in: %f seconds' % self.last_exec_time)

        wait_time = self.last_exec_time - time.time()
        with self._conditionObj:
            self._conditionObj.wait(wait_time)
            # if not interrupted (i.e a new task from handler is available) handle the due task
            if not self._new_task:
                self._handler.process_done_tasks(tasks)

    '''
        Recieve a new task from handler. Check if it has due time lower than the current
        tasks' due time. If it does, schedule it to be next. Else do nothing.
    '''
    def put_task(self, task):
        logging.debug('checking if new task is next')
        if self.last_exec_time >= task.execution_time:
            logging.debug('new task is next')
            with self._conditionObj:
                self._new_task = task
                self._conditionObj.notify()

    '''
        Stop the scheduler loop
    '''
    def stop(self):
        with self._conditionObj:
            self._conditionObj.notify()
            self._stop_event.set()
