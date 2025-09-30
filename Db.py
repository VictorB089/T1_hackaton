import sqlite3
from typing import *

DB_FILENAME = "log_last.db"


class LogDatabase:

    def __init__(self,db_name: str = DB_FILENAME):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self._create_table_()

    def _create_table_(self)->None:
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id TEXT,
                level TEXT,
                message TEXT,
                timestamp TEXT,
                module TEXT,
                caller TEXT,
                tf_proto_version TEXT,
                tf_provider_addr TEXT,
                tf_rpc TEXT,
                tf_resource_type TEXT,
                tf_attribute_path TEXT,
                tf_client_capability_write_only_attributes_allowed BOOLEAN,
                tf_client_capability_deferral_allowed BOOLEAN,
                tf_req_duration_ms INTEGER,
                diagnostic_error_count INTEGER,
                diagnostic_warning_count INTEGER
                            )
        ''')
        self.conn.commit()

    def insert_log(self, logs_data: List[Dict[str, Any]])->None:
        data=[[(log.get('id')),(log.get('level')),(log.get('message')),(log.get('timestamp')),(log.get('module')),(log.get('caller')),(log.get('tf_proto_version')),(log.get('tf_provider_addr')),(log.get('tf_rpc')),(log.get('tf_resource_type')),(log.get('tf_attribute_path')),(log.get('tf_client_capability_write_only_attributes_allowed')),(log.get('tf_client_capability_deferral_allowed')),(log.get('tf_req_duration_ms')),(log.get('diagnostic_error_count')),(log.get('diagnostic_warning_count'))] for log in logs_data]
        self.cursor.executemany('''
            INSERT INTO logs (id, level, message, timestamp, module, caller, tf_proto_version, tf_provider_addr, tf_rpc, tf_resource_type, tf_attribute_path, tf_client_capability_write_only_attributes_allowed, tf_client_capability_deferral_allowed, tf_req_duration_ms, diagnostic_error_count, diagnostic_warning_count)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', data )
        self.conn.commit()
    
    def get_all_logs(self)->List[Dict:[str,Any]]:
        self.cursor.execute('SELECT * FROM logs')
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns,rows)) for row in rows]
    
    def close(self)->None:
        self.conn.close()
    