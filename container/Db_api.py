import Db,sqlite3
from fastapi import FastAPI,Query


LOG_PATH = "test_log.json"
DB_PATH = "output/log_last.db"

app = FastAPI()

@app.get("/logs/filter")
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

db = Db.LogDatabase(DB_PATH)
db.insert_log(Db.import_logs(LOG_PATH))
db.close()