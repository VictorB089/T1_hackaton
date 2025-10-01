import Db
from fastapi import FastAPI


LOG_PATH = "test_log.json"
DB_PATH = "output/log_last.db"

app = FastAPI()

db = Db.LogDatabase(DB_PATH)
db.insert_log(Db.import_logs(LOG_PATH))
db.close()