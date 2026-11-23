import json
import os
import time
from datetime import datetime, timezone

import redis

from src.config.settings import REDIS_URL
from src.services.algorithm import run_case_analysis
from src.utils.db import execute, fetch_one, wait_for_db

QUEUE = 'museum_case_anomaly_jobs'
HEARTBEAT = '/shared/worker-heartbeat'


def heartbeat():
    os.makedirs('/shared', exist_ok=True)
    with open(HEARTBEAT, 'w', encoding='utf-8') as handle:
        handle.write(datetime.now(timezone.utc).isoformat())


def main():
    wait_for_db()
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    while True:
        heartbeat()
        item = client.blpop(QUEUE, timeout=5)
        if not item:
            continue
        _, payload = item
        try:
            job = json.loads(payload)
            run_id = int(job['run_id'])
            case_id = int(job['case_id'])
        except Exception:
            continue
        try:
            # 任务在排队时已快照展品占用区间；即使此刻展柜已换展品，仍按原展品评分。
            claimed = execute(
                "UPDATE algorithm_runs SET status='running', updated_at=NOW() WHERE id=%s AND status='queued'",
                (run_id,),
            )
            if not claimed:
                continue
            run_case_analysis(case_id, run_id)
        except Exception as exc:
            execute(
                "UPDATE algorithm_runs SET status='failed', error=%s, updated_at=NOW() WHERE id=%s",
                (str(exc), run_id),
            )


if __name__ == '__main__':
    main()
