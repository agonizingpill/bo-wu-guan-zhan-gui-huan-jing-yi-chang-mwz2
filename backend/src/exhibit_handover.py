"""展品对调交接：按交接时间划分采样归属，隔离两件展品的评分窗口。

设计要点：
- 每个展柜同一时刻只有一条 ``left_at IS NULL`` 的展品占用区间。
- 采样按 ``sampled_at`` 落入的占用区间归属展品；交接事务会重算两柜采样归属。
- 算法任务在排队时快照当时的占用区间；已排队的任务始终停留在原展品。
- 历史告警在发生当时写入展品快照，后续换柜不会改写。
"""

from datetime import datetime, timezone
import json
from pathlib import Path

import psycopg2
import psycopg2.extras
from fastapi import HTTPException

from src.utils.db import execute, fetch_all, fetch_one, get_conn

MIGRATION_FILE = Path(__file__).resolve().parents[1] / 'migrations' / '002_exhibit_handover.sql'
ADVISORY_LOCK_KEY = 1069002
REQUIRED_TRAINING_SAMPLES = 6


def as_utc(value):
    """统一按 UTC 处理；无时区输入按 UTC 解释。"""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def ensure_schema():
    """幂等地补齐交接相关表、列与历史归属。"""
    sql = MIGRATION_FILE.read_text(encoding='utf-8')
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(%s)', (ADVISORY_LOCK_KEY,))
            cur.execute(sql)


def current_occupancy(case_id):
    return fetch_one(
        '''SELECT * FROM exhibit_occupancies
           WHERE case_id=%s AND left_at IS NULL ORDER BY entered_at DESC LIMIT 1''',
        (case_id,),
    )


def _row(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchone()


def _ensure_occupancy_row(cur, case_id):
    """在当前事务内确保展柜有在柜展品，返回占用区间。"""
    occupancy = _row(
        cur,
        '''SELECT * FROM exhibit_occupancies
           WHERE case_id=%s AND left_at IS NULL ORDER BY entered_at DESC LIMIT 1''',
        (case_id,),
    )
    if occupancy:
        return dict(occupancy)
    case = _row(cur, 'SELECT * FROM display_cases WHERE id=%s', (case_id,))
    if not case:
        raise HTTPException(404, '展柜不存在')
    occupancy = _row(
        cur,
        '''INSERT INTO exhibit_occupancies (case_id, artifact_code, artifact_title, entered_at)
           SELECT %s,%s,%s,
                  LEAST(%s, COALESCE((SELECT MIN(sampled_at) FROM environment_samples WHERE case_id=%s), %s))
           WHERE NOT EXISTS (SELECT 1 FROM exhibit_occupancies WHERE case_id=%s AND left_at IS NULL)
           ON CONFLICT DO NOTHING
           RETURNING *''',
        (case_id, case['artifact_code'], case['artifact_title'],
         case['created_at'], case_id, case['created_at'], case_id),
    )
    if occupancy:
        return dict(occupancy)
    return dict(_row(
        cur,
        '''SELECT * FROM exhibit_occupancies
           WHERE case_id=%s AND left_at IS NULL ORDER BY entered_at DESC LIMIT 1''',
        (case_id,),
    ))


def _occupancy_at_with(cur, case_id, sampled_at):
    sampled_at = as_utc(sampled_at)
    row = _row(
        cur,
        '''SELECT * FROM exhibit_occupancies
           WHERE case_id=%s AND entered_at<=%s AND (left_at IS NULL OR %s<left_at)
           ORDER BY entered_at DESC LIMIT 1''',
        (case_id, sampled_at, sampled_at),
    )
    if row:
        return dict(row)
    row = _row(
        cur,
        'SELECT * FROM exhibit_occupancies WHERE case_id=%s ORDER BY entered_at LIMIT 1',
        (case_id,),
    )
    return dict(row) if row else None


def ensure_case_occupancy(case_id):
    """确保展柜有一条在柜展品；缺失时以展柜档案建立首条区间。"""
    existing = current_occupancy(case_id)
    if existing:
        return existing
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            return _ensure_occupancy_row(cur, case_id)


def occupancy_at(case_id, sampled_at):
    """按采样时间定位当时在柜的展品；早于首条区间时回落到最早展品。"""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            return _occupancy_at_with(cur, case_id, sampled_at)


def create_sample_with_attribution(case_id, sampled_at, temperature, humidity, co2_ppm, door_open, note, created_by):
    """在单一事务中锁定展柜、归属展品并写入采样。

    对展柜行取共享锁：交接事务会持有排他锁，因此采样不会与交接交叉归属。
    采样间隔只在同一件展品的占用区间内校验，换柜后重新起算。
    """
    sampled_at = as_utc(sampled_at)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            case = _row(cur, 'SELECT * FROM display_cases WHERE id=%s FOR SHARE', (case_id,))
            if not case:
                raise HTTPException(404, '展柜不存在')
            if case['status'] != 'active':
                raise HTTPException(409, '停用或维护中的展柜不能接收采样')
            _ensure_occupancy_row(cur, case_id)
            # 按采样时间归属当时在柜的展品，补录历史时段不会被记到新展品名下。
            occupancy = _occupancy_at_with(cur, case_id, sampled_at)
            occupancy_id = occupancy['id'] if occupancy else None
            previous = _row(
                cur,
                '''SELECT sampled_at FROM environment_samples
                   WHERE case_id=%s AND occupancy_id IS NOT DISTINCT FROM %s
                   ORDER BY sampled_at DESC LIMIT 1''',
                (case_id, occupancy_id),
            )
            if previous and (sampled_at - as_utc(previous['sampled_at'])).total_seconds() > 30 * 60:
                raise HTTPException(422, '同一展柜采样间隔不能超过 30 分钟，请补齐缺测时段')
            try:
                row = _row(
                    cur,
                    '''INSERT INTO environment_samples
                       (case_id, sampled_at, temperature, humidity, co2_ppm, door_open, source, state, note, created_by, occupancy_id)
                       VALUES (%s,%s,%s,%s,%s,%s,'manual','accepted',%s,%s,%s) RETURNING *''',
                    (case_id, sampled_at, temperature, humidity, co2_ppm, door_open, note, created_by, occupancy_id),
                )
            except psycopg2.errors.UniqueViolation as exc:
                raise HTTPException(409, '同一时间点已存在采样') from exc
            cur.execute('UPDATE display_cases SET last_sample_at=%s WHERE id=%s', (sampled_at, case_id))
            return dict(row), occupancy


def create_analysis_run(case_id, run_key, parameters):
    """在单一事务中锁定展柜并在排队瞬间快照当时的展品占用区间。"""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            case = _row(cur, 'SELECT * FROM display_cases WHERE id=%s FOR SHARE', (case_id,))
            if not case:
                raise HTTPException(404, '展柜不存在')
            occupancy = _ensure_occupancy_row(cur, case_id)
            if not occupancy:
                raise HTTPException(409, '展柜尚无在柜展品记录，不能建立评分窗口')
            return dict(_row(
                cur,
                '''INSERT INTO algorithm_runs
                   (run_key, case_id, status, input_summary, parameters,
                    occupancy_id, artifact_code, artifact_title, window_start)
                   VALUES (%s,%s,'queued',%s::jsonb,%s::jsonb,%s,%s,%s,%s) RETURNING *''',
                (run_key, case_id,
                 json.dumps({'case_id': case_id, 'artifact_code': occupancy['artifact_code']}),
                 json.dumps(parameters),
                 occupancy['id'], occupancy['artifact_code'], occupancy['artifact_title'], occupancy['entered_at']),
            ))


def ensure_initial_occupancies():
    """为缺少在柜展品的展柜补建首条占用区间。"""
    execute(
        '''INSERT INTO exhibit_occupancies (case_id, artifact_code, artifact_title, entered_at)
           SELECT c.id, c.artifact_code, c.artifact_title,
                  LEAST(c.created_at, COALESCE(
                      (SELECT MIN(s.sampled_at) FROM environment_samples s WHERE s.case_id=c.id),
                      c.created_at))
           FROM display_cases c
           WHERE NOT EXISTS (
               SELECT 1 FROM exhibit_occupancies o WHERE o.case_id=c.id AND o.left_at IS NULL
           )
           ON CONFLICT DO NOTHING'''
    )


def backfill_attribution():
    """按采样时间校准归属，并补写尚未归属的历史告警。

    归属完全由采样时间落入的占用区间决定，因此重复执行是幂等的，也能修正
    早期误归属的采样。
    """
    execute(
        '''UPDATE environment_samples s
           SET occupancy_id = COALESCE(
               (SELECT o.id FROM exhibit_occupancies o
                WHERE o.case_id=s.case_id AND o.entered_at<=s.sampled_at
                ORDER BY o.entered_at DESC LIMIT 1),
               (SELECT o.id FROM exhibit_occupancies o
                WHERE o.case_id=s.case_id ORDER BY o.entered_at LIMIT 1))
           WHERE s.occupancy_id IS DISTINCT FROM COALESCE(
               (SELECT o.id FROM exhibit_occupancies o
                WHERE o.case_id=s.case_id AND o.entered_at<=s.sampled_at
                ORDER BY o.entered_at DESC LIMIT 1),
               (SELECT o.id FROM exhibit_occupancies o
                WHERE o.case_id=s.case_id ORDER BY o.entered_at LIMIT 1))'''
    )
    execute(
        '''UPDATE anomaly_events e
           SET occupancy_id = COALESCE(e.occupancy_id, s.occupancy_id),
               artifact_code = COALESCE(e.artifact_code, o.artifact_code),
               artifact_title = COALESCE(e.artifact_title, o.artifact_title)
           FROM environment_samples s
           LEFT JOIN exhibit_occupancies o ON o.id=s.occupancy_id
           WHERE e.sample_id=s.id AND (e.occupancy_id IS NULL OR e.artifact_code IS NULL)'''
    )


def _exchange_payload_matches(exchange, case_a_id, case_b_id, handed_over_at):
    return (
        exchange['case_a_id'] == case_a_id
        and exchange['case_b_id'] == case_b_id
        and as_utc(exchange['handed_over_at']) == handed_over_at
    )


def _exchange_view(cur, exchange_id):
    cur.execute('SELECT * FROM exhibit_exchanges WHERE id=%s', (exchange_id,))
    exchange = cur.fetchone()
    cur.execute(
        '''SELECT * FROM exhibit_occupancies WHERE exchange_id=%s ORDER BY case_id''',
        (exchange_id,),
    )
    occupancies = [dict(row) for row in cur.fetchall()]
    return {'exchange': dict(exchange), 'occupancies': occupancies, 'idempotent': False}


def confirm_handover(case_a_id, case_b_id, handed_over_at, note, request_key, user):
    """主管确认两柜展品对调。

    整个交接在单一事务中完成：任一校验或写入失败都会回滚，两柜保持原状。
    ``request_key`` 用于幂等重试，成功登记后重复提交同一键不会再次登记。
    """
    if case_a_id == case_b_id:
        raise HTTPException(422, '两柜交接必须选择两个不同的展柜')
    if not user or user.get('role') != 'supervisor':
        raise HTTPException(403, '只有主管可以确认两柜交接')
    handed_over_at = as_utc(handed_over_at)
    now = datetime.now(timezone.utc)
    if handed_over_at > now:
        raise HTTPException(422, '交接时间不能晚于当前时间')

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('SELECT * FROM exhibit_exchanges WHERE request_key=%s', (request_key,))
            existing = cur.fetchone()
            if existing:
                if not _exchange_payload_matches(existing, case_a_id, case_b_id, handed_over_at):
                    raise HTTPException(409, '同一重试凭据对应不同的交接内容，请重新发起')
                return _exchange_view(cur, existing['id']) | {'idempotent': True}

            # 按展柜 id 顺序加锁，避免并发交接互相死锁。
            cur.execute(
                'SELECT * FROM display_cases WHERE id IN (%s,%s) ORDER BY id FOR UPDATE',
                (case_a_id, case_b_id),
            )
            cases = {row['id']: dict(row) for row in cur.fetchall()}
            if len(cases) != 2:
                raise HTTPException(404, '交接的展柜不存在')
            for cid in (case_a_id, case_b_id):
                if cases[cid]['status'] != 'active':
                    raise HTTPException(409, f"展柜 {cases[cid]['code']} 不在岗，不能办理展品交接")

            cur.execute(
                '''SELECT * FROM exhibit_occupancies
                   WHERE case_id IN (%s,%s) AND left_at IS NULL ORDER BY case_id FOR UPDATE''',
                (case_a_id, case_b_id),
            )
            occupancy = {row['case_id']: dict(row) for row in cur.fetchall()}
            for cid in (case_a_id, case_b_id):
                if cid not in occupancy:
                    case = cases[cid]
                    cur.execute(
                        '''INSERT INTO exhibit_occupancies (case_id, artifact_code, artifact_title, entered_at)
                           VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING *''',
                        (cid, case['artifact_code'], case['artifact_title'], case['created_at']),
                    )
                    row = cur.fetchone()
                    if row:
                        occupancy[cid] = dict(row)
            if len(occupancy) != 2:
                raise HTTPException(409, '两柜的在柜展品状态已变化，请刷新后重试')

            current_a, current_b = occupancy[case_a_id], occupancy[case_b_id]
            if current_a['artifact_code'] == current_b['artifact_code']:
                raise HTTPException(422, '两柜当前展品相同，无需交接')
            if handed_over_at <= as_utc(current_a['entered_at']) or handed_over_at <= as_utc(current_b['entered_at']):
                raise HTTPException(422, '交接时间必须晚于两柜当前展品的入柜时间')

            cur.execute(
                '''INSERT INTO exhibit_exchanges
                   (request_key, case_a_id, case_b_id, artifact_a_code, artifact_a_title,
                    artifact_b_code, artifact_b_title, handed_over_at, confirmed_by, note)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (request_key) DO NOTHING RETURNING *''',
                (request_key, case_a_id, case_b_id,
                 current_a['artifact_code'], current_a['artifact_title'],
                 current_b['artifact_code'], current_b['artifact_title'],
                 handed_over_at, user['id'], note),
            )
            exchange = cur.fetchone()
            if not exchange:
                # 并发重试：另一事务已登记同一 request_key。
                cur.execute('SELECT * FROM exhibit_exchanges WHERE request_key=%s', (request_key,))
                exchange = cur.fetchone()
                if not _exchange_payload_matches(exchange, case_a_id, case_b_id, handed_over_at):
                    raise HTTPException(409, '同一重试凭据对应不同的交接内容，请重新发起')
                return _exchange_view(cur, exchange['id']) | {'idempotent': True}

            cur.execute(
                '''UPDATE exhibit_occupancies SET left_at=%s
                   WHERE id IN (%s,%s) AND left_at IS NULL''',
                (handed_over_at, current_a['id'], current_b['id']),
            )
            if cur.rowcount != 2:
                raise HTTPException(409, '两柜的在柜展品状态已变化，请刷新后重试')

            cur.executemany(
                '''INSERT INTO exhibit_occupancies
                   (case_id, artifact_code, artifact_title, entered_at, exchange_id)
                   VALUES (%s,%s,%s,%s,%s)''',
                [
                    (case_a_id, current_b['artifact_code'], current_b['artifact_title'], handed_over_at, exchange['id']),
                    (case_b_id, current_a['artifact_code'], current_a['artifact_title'], handed_over_at, exchange['id']),
                ],
            )
            cur.executemany(
                'UPDATE display_cases SET artifact_code=%s, artifact_title=%s WHERE id=%s',
                [
                    (current_b['artifact_code'], current_b['artifact_title'], case_a_id),
                    (current_a['artifact_code'], current_a['artifact_title'], case_b_id),
                ],
            )

            # 以交接时间为界重算两柜采样归属；等于交接时间的新时段归入柜展品。
            cur.execute(
                '''UPDATE environment_samples s
                   SET occupancy_id=o.id
                   FROM exhibit_occupancies o
                   WHERE s.case_id IN (%s,%s) AND o.case_id=s.case_id
                     AND o.entered_at<=s.sampled_at
                     AND (o.left_at IS NULL OR s.sampled_at<o.left_at)
                     AND s.occupancy_id IS DISTINCT FROM o.id''',
                (case_a_id, case_b_id),
            )

            # 已排队的评分固定到原展品：冻结其评分窗口结束时间。
            cur.execute(
                '''UPDATE algorithm_runs SET window_end=%s, updated_at=NOW()
                   WHERE case_id IN (%s,%s) AND occupancy_id IN (%s,%s)
                     AND status IN ('queued','running')
                     AND (window_end IS NULL OR window_end>%s)''',
                (handed_over_at, case_a_id, case_b_id, current_a['id'], current_b['id'], handed_over_at),
            )

            cur.execute(
                '''INSERT INTO audit_logs (actor_id, action, entity_type, entity_id, detail)
                   VALUES (%s,'confirm_exhibit_handover','exhibit_exchange',%s,%s::jsonb)''',
                (user['id'], exchange['id'], json.dumps({
                    'case_a_id': case_a_id, 'case_b_id': case_b_id,
                    'handed_over_at': handed_over_at.isoformat(),
                    'artifact_a': current_a['artifact_code'], 'artifact_b': current_b['artifact_code'],
                }, ensure_ascii=False)),
            )

            return _exchange_view(cur, exchange['id'])


def training_status(case_id):
    """展柜页面使用的换柜后训练状态。"""
    current = current_occupancy(case_id)
    if not current:
        return {
            'current_occupancy': None,
            'sample_count': 0,
            'required_samples': REQUIRED_TRAINING_SAMPLES,
            'ready': False,
            'latest_run': None,
            'runs': [],
        }
    count = fetch_one(
        '''SELECT COUNT(*) AS count FROM environment_samples
           WHERE case_id=%s AND state='accepted'
             AND (occupancy_id=%s OR (occupancy_id IS NULL AND sampled_at>=%s))''',
        (case_id, current['id'], current['entered_at']),
    )['count']
    runs = fetch_all(
        '''SELECT * FROM algorithm_runs WHERE case_id=%s AND occupancy_id=%s
           ORDER BY created_at DESC, id DESC LIMIT 10''',
        (case_id, current['id']),
    )
    return {
        'current_occupancy': current,
        'sample_count': count,
        'required_samples': REQUIRED_TRAINING_SAMPLES,
        'ready': count >= REQUIRED_TRAINING_SAMPLES,
        'latest_run': runs[0] if runs else None,
        'runs': runs,
    }
