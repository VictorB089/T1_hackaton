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

def import_logs(self,log_path:str = LOG_PATH)->List[Dict[str, Any]]:
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
                        "diagnostic_warning_count":log_entry.get("diagnostic_warning_count") })
                except json.JSONDecodeError:
                    print(f"ошибка чтения JSON на строке:{line_number},строка:\n {line}")
                    failed_lines.append(line_number)
                print(f"Импортировано {len(logs_hold)} строк логов")
                if len(failed_lines)>0: print(f"Строки с ошибками: {line_number}")
                return logs_hold

@app.get("logs/filter")
def get_filtered_logs(
    limit:int = 100,
    id:str | None = Query(None),
    level:str | None = Query(None),
    message:str | None = Query(None),
    timestamp:str | None = Query(None),
    module:str | None = Query(None),
    caller:str | None = Query(None),
    tf_proto_version:str | None = Query(None),
    tf_provider_addr:str | None = Query(None),
    tf_rpc:str | None = Query(None),
    tf_resource_type:str | None = Query(None),
    tf_attribute_path:str | None = Query(None),
    tf_client_capability_write_only_attributes_allowed:bool | None = Query(None),
    tf_client_capability_deferral_allowed:bool | None = Query(None),
    tf_req_duration_ms:int | None = Query(None),
    diagnostic_error_count:int | None = Query(None),
    diagnostic_warning_count:int | None = Query(None)
    ):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    sql = "SELECT id, level, message, timestamp, module, caller, tf_proto_version, tf_provider_addr, tf_rpc, tf_resource_type, tf_attribute_path, tf_client_capability_write_only_attributes_allowed, tf_client_capability_deferral_allowed, tf_req_duration_ms, diagnostic_error_count, diagnostic_warning_count FROM logs"
    columns:list = [id, level, message, timestamp, module, caller, tf_proto_version, tf_provider_addr, tf_rpc, tf_resource_type, tf_attribute_path, tf_client_capability_write_only_attributes_allowed, tf_client_capability_deferral_allowed, tf_req_duration_ms, diagnostic_error_count, diagnostic_warning_count]
    filters:list[str] = []
    params:list = []

    for col in columns:
        if col:
            filters.append(f"{col} = ?")
            params.append(col)
    
    if filters:
        sql += " WHERE " + " AND ".join(filters)
    sql += " ORDER BY timestamp DESC LIMIT ? "
    params.append(limit)

    cursor.execute(sql,params)
    rows = cursor.fetchall()
    conn.close()

    return [
        {"id":r[0], "level":r[1], "message":r[2], "timestamp":r[3], "module":r[4], "caller":r[5], "tf_proto_version":r[6], "tf_provider_addr":r[7], "tf_rpc":r[8], "tf_resource_type":r[9], "tf_attribute_path":r[10], "tf_client_capability_write_only_attributes_allowed":r[11], "tf_client_capability_deferral_allowed":r[12], "tf_req_duration_ms":r[13], "diagnostic_error_count":r[14], "diagnostic_warning_count":r[15]} for r in rows
    ]

if __name__ == "__main__":
    db = LogDatabase(DB_PATH)
    db.insert_log(import_logs(LOG_PATH))
    