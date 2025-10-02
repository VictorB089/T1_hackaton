test_log.json в папке container парсится в БД при запуске контейнера
build_container.bat - собрать контейнер
start_container.bat - запустить контейнер
REST api:
GET  по http://localhost:8000/logs/filter?
фильтры по {ключ}="значение"&{ключ}="значение"
"http://localhost:8000/logs/filter?{ключ}=значение&{ключ}=значение"
