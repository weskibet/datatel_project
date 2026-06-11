INSERT INTO etl_watermarks (
    pipeline_name,
    loaded_until,
    updated_at
)
VALUES (
    'datatel_daily_pipeline',
    %(window_end)s::timestamptz,
    now()
)
ON CONFLICT (pipeline_name) DO UPDATE
SET loaded_until = greatest(etl_watermarks.loaded_until, EXCLUDED.loaded_until),
    updated_at = now();
