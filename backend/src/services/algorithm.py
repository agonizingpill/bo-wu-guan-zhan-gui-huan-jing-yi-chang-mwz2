from datetime import datetime, timezone
import json

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

from src.utils.db import clean, execute, execute_returning, fetch_all, fetch_one


RANDOM_STATE = 1069
CONTAMINATION = 0.22


def run_case_analysis(case_id: int, run_id: int):
    run = fetch_one('SELECT * FROM algorithm_runs WHERE id=%s', (run_id,))
    if not run:
        raise ValueError('算法任务不存在')
    occupancy_id = run['occupancy_id']
    window_start = run['window_start']
    window_end = run['window_end']
    occupancy = None
    if occupancy_id is not None:
        occupancy = fetch_one('SELECT * FROM exhibit_occupancies WHERE id=%s', (occupancy_id,))
        if window_start is None and occupancy:
            window_start = occupancy['entered_at']

    # 评分只使用本次入柜后的读数：按展品占用区间与交接时间窗取数。
    conditions = ["s.case_id=%s", "s.state='accepted'"]
    params = [case_id]
    if occupancy_id is not None:
        conditions.append("s.occupancy_id=%s")
        params.append(occupancy_id)
    if window_start is not None:
        conditions.append('s.sampled_at>=%s')
        params.append(window_start)
    if window_end is not None:
        conditions.append('s.sampled_at<%s')
        params.append(window_end)
    samples = fetch_all(
        '''SELECT s.*, c.target_temp_min, c.target_temp_max, c.target_rh_min, c.target_rh_max
           FROM environment_samples s
           JOIN display_cases c ON c.id=s.case_id
           LEFT JOIN exhibit_occupancies o ON o.id=s.occupancy_id
           WHERE ''' + ' AND '.join(conditions)
        + ''' AND (o.id IS NULL OR (s.sampled_at>=o.entered_at AND (o.left_at IS NULL OR s.sampled_at<o.left_at)))
           ORDER BY s.sampled_at''',
        tuple(params),
    )
    artifact_code = run['artifact_code'] or (occupancy['artifact_code'] if occupancy else None)
    artifact_title = run['artifact_title'] or (occupancy['artifact_title'] if occupancy else None)
    if len(samples) < 6:
        raise ValueError('本次入柜后的有效采样不足 6 条，无法建立异常基线，请补齐换柜后采样')

    features = np.array([[float(row['temperature']), float(row['humidity']), float(row['co2_ppm'])] for row in samples])
    scaler = RobustScaler()
    scaled = scaler.fit_transform(features)
    model = IsolationForest(contamination=CONTAMINATION, random_state=RANDOM_STATE, n_estimators=160)
    labels = model.fit_predict(scaled)
    scores = model.decision_function(scaled)
    threshold = float(np.quantile(scores, CONTAMINATION))
    previous_flagged = 0
    created_events = []

    for row, label, score in zip(samples, labels, scores):
        abnormal = label == -1
        if abnormal:
            previous_flagged += 1
        else:
            previous_flagged = 0
        if not abnormal:
            continue

        door_context = bool(row['door_open'])
        continuous = previous_flagged >= 3
        anomaly_type = 'door_interference' if door_context else ('continuous_drift' if continuous else 'environment_outlier')
        severity = 'watch' if door_context else ('critical' if continuous else 'high')
        escalated = continuous and not door_context
        status = 'escalated' if escalated else 'open'
        reason = (
            '门磁处于开启段，模型异常只作为开门干扰观察，不判定为设备故障'
            if door_context else
            ('连续异常达到 3 个采样点，已升级馆长查看' if continuous else '环境向量偏离展柜稳态基线')
        )
        existing = fetch_one('SELECT id FROM anomaly_events WHERE sample_id=%s', (row['id'],))
        if existing:
            execute(
                '''UPDATE anomaly_events SET algorithm_run_id=%s, score=%s, threshold=%s,
                   anomaly_type=%s, severity=%s, status=%s, door_context=%s,
                   consecutive_count=%s, escalated_to_curator=%s, reason=%s,
                   occupancy_id=COALESCE(occupancy_id,%s),
                   artifact_code=COALESCE(artifact_code,%s),
                   artifact_title=COALESCE(artifact_title,%s)
                   WHERE id=%s''',
                (run_id, float(score), threshold, anomaly_type, severity, status, door_context, previous_flagged,
                 escalated, reason, row['occupancy_id'], artifact_code, artifact_title, existing['id']),
            )
            event_id = existing['id']
        else:
            event = execute_returning(
                '''INSERT INTO anomaly_events
                   (case_id, sample_id, algorithm_run_id, score, threshold, anomaly_type, severity,
                    status, door_context, consecutive_count, escalated_to_curator, reason, created_by,
                    occupancy_id, artifact_code, artifact_title)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,%s) RETURNING id''',
                (case_id, row['id'], run_id, float(score), threshold, anomaly_type, severity, status, door_context,
                 previous_flagged, escalated, reason, row['occupancy_id'], artifact_code, artifact_title),
            )
            event_id = event['id']
        created_events.append({'event_id': event_id, 'sample_id': row['id'], 'status': status, 'type': anomaly_type, 'score': float(score)})

    metrics = {
        'sample_count': len(samples),
        'outlier_count': len(created_events),
        'outlier_rate': round(len(created_events) / len(samples), 4),
        'threshold': round(threshold, 5),
        'door_interference_count': sum(1 for item in created_events if item['type'] == 'door_interference'),
        'continuous_upgrade_count': sum(1 for item in created_events if item['status'] == 'escalated'),
        'occupancy_id': occupancy_id,
        'artifact_code': artifact_code,
        'window_start': window_start.isoformat() if window_start else None,
        'window_end': window_end.isoformat() if window_end else None,
    }
    result = {
        'events': created_events,
        'artifact_code': artifact_code,
        'artifact_title': artifact_title,
        'scored_from': samples[0]['sampled_at'].isoformat() if samples else None,
        'scored_to': samples[-1]['sampled_at'].isoformat() if samples else None,
        'feature_order': ['temperature_c', 'relative_humidity_pct', 'co2_ppm'],
        'scaler': 'RobustScaler(median/IQR)',
        'detector': 'IsolationForest(n_estimators=160, contamination=0.22, random_state=1069)',
        'completed_at': datetime.now(timezone.utc).isoformat(),
    }
    execute(
        '''UPDATE algorithm_runs SET status='succeeded', metrics=%s::jsonb, result=%s::jsonb,
           error=NULL, updated_at=NOW() WHERE id=%s''',
        (json.dumps(clean(metrics), ensure_ascii=False), json.dumps(clean(result), ensure_ascii=False), run_id),
    )
    return metrics, result
