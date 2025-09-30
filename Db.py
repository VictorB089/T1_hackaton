import sqlite3
from typing import *

DB_FILENAME = "log_last.db"


class LogDatabase:

    def __init__(self,db_name: str = DB_FILENAME):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self)->None:
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                last_task_id INTEGER,
                level TEXT,
                message TEXT,
                timestamp DATETIME,
                module TEXT,
                caller TEXT,
                extra_json TEXT
                            )
        ''')
        self.conn.commit()

    def insert_log(self, logs_data: List[Dict[str, Any]])->None:
        data=[(logs_data.get('id')),(logs_data.get('last_task_id')),(logs_data.get('level')),(logs_data.get('message')),(logs_data.get('timestamp')),(logs_data.get('module')),(logs_data.get('module')),(logs_data.get('caller')),(logs_data.get('extra_json')) for log in logs_data]
        self.cursor.executemany('''
            INSERT INTO logs (id, last_task_id, level, message, timestamp, module, caller, extra_json)
            VALUES (?,?,?,?,?,?,?,?)
        ''', (
        ))
        self.conn.commit()
    
    def get_all_logs(self)->List[Dict:[str,Any]]:
        self.cursor.execute('SELECT * FROM logs')
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns,rows)) for row in rows]
    
    def close(self)->None:
        self.conn.close()
    