from typing import *
from fastapi import Query, FastAPI
import sqlite3,json

LOG_PATH = "test_log.json"
DB_PATH = "output/log_last.db"

app = FastAPI()

class LogDatabase:

    def __init__(self,db_name: str = DB_PATH):
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
                diagnostic_warning_count INTEGER,
                incomplete TEXT
                            )
        ''')
        self.conn.commit()

    def insert_log(self, logs_data: List[Dict[str, Any]])->None:
        data=[[(log.get('id')),(log.get('level')),(log.get('message')),(log.get('timestamp')),(log.get('module')),(log.get('caller')),(log.get('tf_proto_version')),(log.get('tf_provider_addr')),(log.get('tf_rpc')),(log.get('tf_resource_type')),(log.get('tf_attribute_path')),(log.get('tf_client_capability_write_only_attributes_allowed')),(log.get('tf_client_capability_deferral_allowed')),(log.get('tf_req_duration_ms')),(log.get('diagnostic_error_count')),(log.get('diagnostic_warning_count')),(log.get('incomplete'))] for log in logs_data]
        self.cursor.executemany('''
            INSERT INTO logs (id, level, message, timestamp, module, caller, tf_proto_version, tf_provider_addr, tf_rpc, tf_resource_type, tf_attribute_path, tf_client_capability_write_only_attributes_allowed, tf_client_capability_deferral_allowed, tf_req_duration_ms, diagnostic_error_count, diagnostic_warning_count, incomplete)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', data )
        self.conn.commit()

    def get_all_logs(self)->List[Dict:[str,Any]]:
        self.cursor.execute('SELECT * FROM logs')
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns,rows)) for row in rows]
    
    def close(self)->None:
        self.conn.close()

def import_logs(log_path:str = LOG_PATH)->List[Dict[str, Any]]:
        logs_hold=[]
        with open(log_path, "r", encoding="utf-8") as f:
            line_number:int = 0
            failed_lines:list[int] = []
            for line in f:
                line_number+=1
                line = line.strip()
                if not line:
                    continue
                try:
                    log_entry = json.loads(line)
                    logs_hold.append({
                        "id": log_entry.get("tf_req_id"),
                        "level":log_entry.get("@level"),
                        "message":log_entry.get("@message"),
                        "timestamp":log_entry.get("@timestamp"),
                        "module":log_entry.get("@module"),
                        "caller":log_entry.get("@caller"),
                        "tf_proto_version":log_entry.get("tf_proto_version"),
                        "tf_provider_addr":log_entry.get("tf_provider_addr"),
                        "tf_rpc":log_entry.get("tf_rpc"),
                        "tf_resource_type":log_entry.get("tf_resource_type"),
                        "tf_attribute_path":log_entry.get("tf_attribute_path"),
                        "tf_client_capability_write_only_attributes_allowed":log_entry.get("tf_client_capability_write_only_attributes_allowed"),
                        "tf_client_capability_deferral_allowed":log_entry.get("tf_client_capability_deferral_allowed"),
                        "tf_req_duration_ms":log_entry.get("tf_req_duration_ms"),
                        "diagnostic_error_count":log_entry.get("diagnostic_error_count"),
                        "diagnostic_warning_count":log_entry.get("diagnostic_warning_count"),
                        "incomplete":log_entry.get("incomplete")})
                except json.JSONDecodeError:
                    logs_hold.append({"incomplete":str(line)})
                    print(f"ошибка чтения JSON на строке:{line_number},строка:\n {line}")
                    failed_lines.append(line_number)
                print(f"Импортировано {len(logs_hold)} строк логов")
                if len(failed_lines)>0: print(f"Строки с ошибками: {line_number}")
                return logs_hold