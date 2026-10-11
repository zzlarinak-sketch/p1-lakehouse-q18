"""Мини-клиент Trino по HTTP (без сторонних библиотек) + общие функции для замеров."""

import json
import subprocess
import time
import urllib.parse
import urllib.request

TRINO = "http://localhost:8080"


class Trino:
    def __init__(self):
        # свойства сессии (SET SESSION) - Trino возвращает их в заголовке ответа,
        # а клиент должен сам передавать их в следующих запросах
        self.session = {}

    def run(self, sql):
        """Выполняет один SQL, возвращает (строки, колонки, время в секундах по данным Trino)."""
        headers = {"X-Trino-User": "trino", "Content-Type": "text/plain"}
        if self.session:
            headers["X-Trino-Session"] = ",".join(
                f"{k}={urllib.parse.quote(v)}" for k, v in self.session.items())
        req = urllib.request.Request(TRINO + "/v1/statement", data=sql.encode("utf-8"),
                                     headers=headers, method="POST")
        rows, cols, stats = [], None, {}
        with urllib.request.urlopen(req) as r:
            self._take_headers(r)
            resp = json.load(r)
        while True:
            rows.extend(resp.get("data", []))
            cols = cols or resp.get("columns")
            stats = resp.get("stats", stats)
            if resp.get("error"):
                raise RuntimeError(resp["error"].get("message", "trino error"))
            nxt = resp.get("nextUri")
            if not nxt:
                break
            for attempt in range(60):
                try:
                    with urllib.request.urlopen(nxt) as r:
                        self._take_headers(r)
                        resp = json.load(r)
                    break
                except Exception:
                    if attempt == 59:
                        raise
                    time.sleep(2)
        names = [c["name"] for c in cols] if cols else []
        return rows, names, stats.get("elapsedTimeMillis", 0) / 1000.0

    def _take_headers(self, r):
        for h in r.headers.get_all("X-Trino-Set-Session") or []:
            k, v = h.split("=", 1)
            self.session[k.strip()] = urllib.parse.unquote(v.strip())

    def scalar(self, sql):
        rows, _, _ = self.run(sql)
        return rows[0][0]


def wait_trino(timeout=300):
    """Ждёт, пока Trino ответит и сможет выполнить запрос (после старта бывает No nodes available)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(TRINO + "/v1/info", timeout=5) as r:
                if json.load(r).get("starting") is False:
                    Trino().run("SELECT 1")
                    return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError("Trino не поднялся")


def make_cold():
    """Холодный старт для замера: перезапуск trino и minio + сброс page cache в VM докера.
    Без сброса кэша файлы, которые только что записали, остаются в памяти, и "холодный"
    прогон на самом деле тёплый."""
    subprocess.run(["docker", "restart", "trino", "lakehouse-minio"], capture_output=True)
    subprocess.run(["docker", "run", "--rm", "--privileged", "alpine",
                    "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"], capture_output=True)
    wait_trino()
    time.sleep(3)


def split_sql(text):
    """Режет sql-файл на отдельные запросы по ';' (комментарии -- выкидываем)."""
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def table_size(t, fq):
    """(строк, файлов, байт) Iceberg-таблицы по метаданным $files."""
    cat, sch, name = fq.split(".")
    rows, _, _ = t.run(
        f'SELECT sum(record_count), count(*), sum(file_size_in_bytes) '
        f'FROM {cat}.{sch}."{name}$files"')
    r = rows[0]
    return int(r[0] or 0), int(r[1] or 0), int(r[2] or 0)
