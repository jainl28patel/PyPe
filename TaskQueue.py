import sqlite3
import threading

class TaskQueue:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.conn = sqlite3.connect('task_queue.db')
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS task_queue
             (task_name text, task_id text)''')
        self.c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS task_id_index
             ON task_queue (task_id)''')
        self.conn.commit()

    def add_task(self, task_name, task_id):
        with self.lock:
            try:
                self.c.execute("INSERT INTO task_queue VALUES (?, ?)", (task_name, task_id))
                self.conn.commit()
            except sqlite3.IntegrityError:
                return "Task already exists in the queue."

    def get_task(self):
        with self.lock:
            try:
                self.c.execute("SELECT * FROM task_queue")
                return self.c.fetchall()
            except sqlite3.IntegrityError:
                return "Task does not exist in the queue."
    
    def remove_task(self, task_id):
        with self.lock:
            try:
                self.c.execute("DELETE FROM task_queue WHERE task_id=?", (task_id,))
                self.conn.commit()
            except sqlite3.IntegrityError:
                return "Task does not exist in the queue."

    def __del__(self):
        self.conn.close()

taskQueue = TaskQueue()