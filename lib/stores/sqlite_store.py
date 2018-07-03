import sqlite3
from lib.models.models import Task


class SQLiteStore:
    CREATE_TABLE_SQL = """ CREATE TABLE IF NOT EXISTS tasks (
                                            id INTEGER PRIMARY KEY,
                                            execution_time REAL NOT NULL,
                                            type TEXT NOT NULL,
                                            data TEXT,
                                            done INTEGER NOT NULL DEFAULT 0,
                                            repeat INTEGER
                                            repeat_type TEXT
                                        ); """

    CREATE_TIME_INDEX_SQL = "CREATE INDEX execution_time_index on tasks (execution_time);"

    GET_NEXT_JOBS_SQL = """ SELECT id, execution_time, type, data, repeat
                            FROM tasks
                            WHERE execution_time = (SELECT min(execution_time) FROM tasks WHERE execution_time > ?) 
                            AND done = 0
                            LIMIT ?; """

    PUT_JOB_SQL = """ INSERT INTO tasks (execution_time,type,data,repeat)
                      VALUES (?,?,?,?); """

    UPDATE_DONE_JOB_SQL = """ UPDATE tasks done = 1, tasks execution_time = ? 
                              WHERE id = ?; """

    def __init__(self):
        self.conn = None

    def setup(self, db):
        try:
            self.conn = sqlite3.connect(db, check_same_thread=False)

            self._create_table()
        except sqlite3.Error as e:
            return {'error': e}

        return None

    def get_next_tasks(self, minimum_execution_time, limit):
        try:
            c = self.conn.cursor()
            c.execute(self.GET_NEXT_JOBS_SQL, (minimum_execution_time, limit))

            next_tasks = c.fetchall()

            next_tasks_list = []

            for task in next_tasks:
                next_tasks_list.append(Task(*task))

            return {'error': None, 'data': next_tasks_list}

        except sqlite3.Error as e:
            return {'error': e}

    def put_task(self, task):
        try:
            c = self.conn.cursor()
            t = (task.execution_time, task.type, task.data, task.repeat)
            c.execute(self.PUT_JOB_SQL, t)
            self.conn.commit()
            return {'error': None}

        except sqlite3.Error as e:
            return {'error': e}

    def update_done_tasks(self, update_tasks):
        sql = """ UPDATE """
        try:
            c = self.conn.cursor()
            c.execute('BEGIN TRANSACTION')

            for update_task in update_tasks:
                if update_task.execution_time:
                    c.execute('UPDATE tasks SET execution_time=? WHERE id=?',
                              (update_task.id, update_task.execution_time))
                else:
                    c.execute('UPDATE tasks SET done=1, WHERE id=?', (update_task.id,))

            c.execute('COMMIT')
            return {'error': None}

        except sqlite3.Error as e:
            return {'error': e}

    def _create_table(self):
        try:
            c = self.conn.cursor()
            c.execute(self.CREATE_TABLE_SQL)
            c.execute(self.CREATE_TIME_INDEX_SQL)

            self.conn.commit()
            return {'error': None}

        except sqlite3.Error as e:
            return {'error': e}
