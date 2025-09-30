from T1_hackaton_repo import Db
import json

TESTLOG_PATH = "test_log.json"
TEST_DB_PATH = "testDB.db"

db = Db.LogDatabase(TEST_DB_PATH)

logs_hold=[]

with open(TESTLOG_PATH, "r", encoding="utf-8") as f:
    line_number:int = 0
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

db.insert_log(logs_hold)
print(f"Импортировано {len(logs_hold)} строк логов")
db.close()