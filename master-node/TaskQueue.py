import sqlite3
import threading

# Create a thread-local data container
mydata = threading.local()

def get_db():
    # Ensure a unique connection per thread
    if not hasattr(mydata, "conn"):
        mydata.conn = sqlite3.connect("task_queue.db")
    return mydata.conn

class TaskQueue:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        conn = get_db()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS task_queue
             (task_name text, task_id text)''')
        c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS task_id_index
             ON task_queue (task_id)''')
        conn.commit()

    def add_task(self, task_name, task_id):
        with self.lock:
            try:
                conn = get_db()
                c = conn.cursor()
                c.execute("INSERT INTO task_queue VALUES (?, ?)", (task_name, task_id))
                conn.commit()
            except sqlite3.IntegrityError:
                return "Task already exists in the queue."

    def get_task(self):
        with self.lock:
            try:
                conn = get_db()
                c = conn.cursor()
                c.execute("SELECT * FROM task_queue")
                json_data = []
                for row in c.fetchall():
                    json_data.append({"task": row[0], "task_id": row[1]})
                return json_data
            except sqlite3.IntegrityError:
                return None
    
    def remove_task(self, task_id):
        with self.lock:
            try:
                conn = get_db()
                c = conn.cursor()
                c.execute("DELETE FROM task_queue WHERE task_id=?", (task_id,))
                conn.commit()
            except sqlite3.IntegrityError:
                return "Task does not exist in the queue."

    def __del__(self):
        conn = get_db()
        conn.close()
