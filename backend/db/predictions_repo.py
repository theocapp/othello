import json

from db.common import _connect


def upsert_prediction_records(records: list[dict]) -> int:
    if not records:
        return 0
    saved = 0
    with _connect() as conn:
        for record in records:
            extracted_subjects = json.dumps(
                record.get("extracted_subjects") or [], sort_keys=True
            )
            payload = json.dumps(record.get("payload") or {}, sort_keys=True)
            conn.execute(
                """
                INSERT INTO prediction_ledger (
                    prediction_key, topic, source_type, source_ref, prediction_text, prediction_horizon_days,
                    prediction_type, extracted_subjects, status, confidence, created_at, horizon_at,
                    resolved_at, outcome_summary, payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (prediction_key) DO UPDATE SET
                    topic = EXCLUDED.topic,
                    source_type = EXCLUDED.source_type,
                    source_ref = EXCLUDED.source_ref,
                    prediction_text = EXCLUDED.prediction_text,
                    prediction_horizon_days = EXCLUDED.prediction_horizon_days,
                    prediction_type = EXCLUDED.prediction_type,
                    extracted_subjects = EXCLUDED.extracted_subjects,
                    status = EXCLUDED.status,
                    confidence = EXCLUDED.confidence,
                    created_at = EXCLUDED.created_at,
                    horizon_at = EXCLUDED.horizon_at,
                    resolved_at = EXCLUDED.resolved_at,
                    outcome_summary = EXCLUDED.outcome_summary,
                    payload = EXCLUDED.payload
                """,
                (
                    record["prediction_key"],
                    record.get("topic"),
                    record["source_type"],
                    record.get("source_ref"),
                    record["prediction_text"],
                    record["prediction_horizon_days"],
                    record.get("prediction_type"),
                    extracted_subjects,
                    record["status"],
                    record.get("confidence"),
                    record["created_at"],
                    record["horizon_at"],
                    record.get("resolved_at"),
                    record.get("outcome_summary"),
                    payload,
                ),
            )
            saved += 1
    return saved


def load_prediction_records(
    topic: str | None = None, status: str | None = None, limit: int = 100
) -> list[dict]:
    params: list[object] = []
    clauses = []
    if topic:
        clauses.append("topic = %s")
        params.append(topic)
    if status:
        clauses.append("status = %s")
        params.append(status)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM prediction_ledger
            {where}
            ORDER BY created_at DESC
            LIMIT %s
            """,
            params,
        ).fetchall()

    result = []
    for row in rows:
        extracted_subjects = row.get("extracted_subjects")
        payload = row.get("payload")
        if isinstance(extracted_subjects, str):
            extracted_subjects = (
                json.loads(extracted_subjects) if extracted_subjects else []
            )
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        result.append(
            {
                "prediction_key": row.get("prediction_key"),
                "topic": row.get("topic"),
                "source_type": row.get("source_type"),
                "source_ref": row.get("source_ref"),
                "prediction_text": row.get("prediction_text"),
                "prediction_horizon_days": row.get("prediction_horizon_days"),
                "prediction_type": row.get("prediction_type"),
                "extracted_subjects": extracted_subjects or [],
                "status": row.get("status"),
                "confidence": row.get("confidence"),
                "created_at": row.get("created_at"),
                "horizon_at": row.get("horizon_at"),
                "resolved_at": row.get("resolved_at"),
                "outcome_summary": row.get("outcome_summary"),
                "payload": payload or {},
            }
        )
    return result


def delete_prediction_records(
    topic: str | None = None, source_ref: str | None = None
) -> int:
    clauses = []
    params: list[object] = []
    if topic:
        clauses.append("topic = %s")
        params.append(topic)
    if source_ref:
        clauses.append("source_ref = %s")
        params.append(source_ref)
    if not clauses:
        return 0
    with _connect() as conn:
        row = conn.execute(
            f"DELETE FROM prediction_ledger WHERE {' AND '.join(clauses)} RETURNING prediction_key",
            params,
        ).fetchall()
        return len(row)
