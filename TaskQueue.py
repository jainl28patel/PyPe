import sqlite3

class TaskQueue:
    def __init__(self) -> None:
        self.conn = sqlite3.connect('task_queue.db')
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS task_queue
             (task_name text, task_id text)''')
        self.c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS task_id_index
             ON task_queue (task_id)''')
        self.conn.commit()

    def add_task(self, task_name, task_id):
        try:
            self.c.execute("INSERT INTO task_queue VALUES (?, ?)", (task_name, task_id))
            self.conn.commit()
        except sqlite3.IntegrityError:
            return "Task already exists in the queue."

    def get_task(self):
        try:
            self.c.execute("SELECT * FROM task_queue")
            return self.c.fetchone()
        except sqlite3.IntegrityError:
            return "Task does not exist in the queue."
    
    def remove_task(self, task_id):
        try:
            self.c.execute("DELETE FROM task_queue WHERE task_id=?", (task_id,))
            self.conn.commit()
        except sqlite3.IntegrityError:
            return "Task does not exist in the queue."

    def __del__(self):
        self.conn.close()

taskQueue = TaskQueue()
taskQueue.add_task("task1", "task1_id")
taskQueue.add_task("task2", "task2_id")

print(taskQueue.get_task())
taskQueue.remove_task("task1_id")
print(taskQueue.get_task())