-- 展品对调交接：按交接时间划分采样归属，评分只使用本次入柜后的读数。

CREATE TABLE IF NOT EXISTS exhibit_exchanges (
  id SERIAL PRIMARY KEY,
  request_key VARCHAR(120) UNIQUE NOT NULL,
  case_a_id INTEGER NOT NULL REFERENCES display_cases(id) ON DELETE RESTRICT,
  case_b_id INTEGER NOT NULL REFERENCES display_cases(id) ON DELETE RESTRICT,
  artifact_a_code VARCHAR(60) NOT NULL,
  artifact_a_title VARCHAR(180) NOT NULL,
  artifact_b_code VARCHAR(60) NOT NULL,
  artifact_b_title VARCHAR(180) NOT NULL,
  handed_over_at TIMESTAMPTZ NOT NULL,
  confirmed_by INTEGER REFERENCES users(id),
  note VARCHAR(500) NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (case_a_id <> case_b_id)
);

CREATE TABLE IF NOT EXISTS exhibit_occupancies (
  id SERIAL PRIMARY KEY,
  case_id INTEGER NOT NULL REFERENCES display_cases(id) ON DELETE CASCADE,
  artifact_code VARCHAR(60) NOT NULL,
  artifact_title VARCHAR(180) NOT NULL,
  entered_at TIMESTAMPTZ NOT NULL,
  left_at TIMESTAMPTZ,
  exchange_id INTEGER REFERENCES exhibit_exchanges(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (left_at IS NULL OR left_at >= entered_at)
);

-- 每个展柜同一时刻只允许一件在柜展品。
CREATE UNIQUE INDEX IF NOT EXISTS uq_current_occupancy ON exhibit_occupancies(case_id) WHERE left_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_occupancy_case_time ON exhibit_occupancies(case_id, entered_at);

ALTER TABLE environment_samples ADD COLUMN IF NOT EXISTS occupancy_id INTEGER REFERENCES exhibit_occupancies(id) ON DELETE SET NULL;
ALTER TABLE anomaly_events ADD COLUMN IF NOT EXISTS occupancy_id INTEGER REFERENCES exhibit_occupancies(id) ON DELETE SET NULL;
ALTER TABLE anomaly_events ADD COLUMN IF NOT EXISTS artifact_code VARCHAR(60);
ALTER TABLE anomaly_events ADD COLUMN IF NOT EXISTS artifact_title VARCHAR(180);
ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS occupancy_id INTEGER REFERENCES exhibit_occupancies(id) ON DELETE SET NULL;
ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS artifact_code VARCHAR(60);
ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS artifact_title VARCHAR(180);
ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ;
ALTER TABLE algorithm_runs ADD COLUMN IF NOT EXISTS window_end TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_samples_occupancy ON environment_samples(occupancy_id, sampled_at);
CREATE INDEX IF NOT EXISTS idx_anomaly_occupancy ON anomaly_events(occupancy_id);
CREATE INDEX IF NOT EXISTS idx_runs_occupancy ON algorithm_runs(case_id, occupancy_id, created_at DESC);

-- 为既有展柜建立首次展品占用区间；入柜时间取展柜创建时间与最早有效采样中较早者。
INSERT INTO exhibit_occupancies (case_id, artifact_code, artifact_title, entered_at)
SELECT c.id, c.artifact_code, c.artifact_title,
       LEAST(c.created_at, COALESCE((SELECT MIN(s.sampled_at) FROM environment_samples s WHERE s.case_id=c.id), c.created_at))
FROM display_cases c
WHERE NOT EXISTS (SELECT 1 FROM exhibit_occupancies o WHERE o.case_id=c.id AND o.left_at IS NULL)
ON CONFLICT DO NOTHING;

-- 既有采样按采样时间归属到当时在柜的展品。
UPDATE environment_samples s
SET occupancy_id = COALESCE(
    (SELECT o.id FROM exhibit_occupancies o
     WHERE o.case_id=s.case_id AND o.entered_at<=s.sampled_at
     ORDER BY o.entered_at DESC LIMIT 1),
    (SELECT o.id FROM exhibit_occupancies o
     WHERE o.case_id=s.case_id ORDER BY o.entered_at LIMIT 1))
WHERE s.occupancy_id IS NULL;

-- 历史告警补写“当时展品”快照。
UPDATE anomaly_events e
SET occupancy_id = COALESCE(e.occupancy_id, s.occupancy_id),
    artifact_code = COALESCE(e.artifact_code, o.artifact_code),
    artifact_title = COALESCE(e.artifact_title, o.artifact_title)
FROM environment_samples s
LEFT JOIN exhibit_occupancies o ON o.id=s.occupancy_id
WHERE e.sample_id=s.id AND (e.occupancy_id IS NULL OR e.artifact_code IS NULL);

-- 既有算法任务补写排队时所对应的展品与评分窗口。
UPDATE algorithm_runs r
SET occupancy_id = COALESCE(r.occupancy_id, (
        SELECT o.id FROM exhibit_occupancies o
        WHERE o.case_id=r.case_id AND o.entered_at<=r.created_at ORDER BY o.entered_at DESC LIMIT 1)),
    artifact_code = COALESCE(r.artifact_code, (
        SELECT o.artifact_code FROM exhibit_occupancies o
        WHERE o.case_id=r.case_id AND o.entered_at<=r.created_at ORDER BY o.entered_at DESC LIMIT 1)),
    artifact_title = COALESCE(r.artifact_title, (
        SELECT o.artifact_title FROM exhibit_occupancies o
        WHERE o.case_id=r.case_id AND o.entered_at<=r.created_at ORDER BY o.entered_at DESC LIMIT 1))
WHERE r.occupancy_id IS NULL OR r.artifact_code IS NULL;

UPDATE algorithm_runs r
SET window_start = COALESCE(r.window_start, o.entered_at)
FROM exhibit_occupancies o
WHERE o.id=r.occupancy_id AND r.window_start IS NULL;
