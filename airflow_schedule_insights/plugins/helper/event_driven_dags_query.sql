WITH RECURSIVE
-- 1. Calculate median duration using only the last 50 successful runs
recent_runs AS (
    SELECT 
        dag_id,
        start_date,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY dag_id ORDER BY start_date DESC) as run_rank
    FROM dag_run
    WHERE state = 'success'
),
dags_durations AS (
    SELECT 
        dag_id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (end_date - start_date)) as mean_duration
    FROM recent_runs
    WHERE run_rank <= 50
    GROUP BY dag_id
),
-- 2. Recursive CTE with explicit column definition
datasets_dependencies (
    dag_id,
    dep_id,
    dep_type,
    trigger_id,
    trigger_type,
    dataset_dependencies,
    condition_type,
    lvl,
    dataset
) AS (
    -- Anchor: Get DAGs with Dataset schedules
    SELECT
        dag_id::varchar,
        dag_id::varchar,
        'DAG'::varchar,
        concat(dag_id, '_root')::varchar,
        'dataset'::varchar,
        "data" -> 'dag' -> 'schedule' as dataset_dependencies,
        'any'::varchar,
        1,
        "data" -> 'dag' -> 'schedule' ->> 'uri'
    FROM serialized_dag
    WHERE is_latest = true 
      AND "data" -> 'dag' -> 'schedule' IS NOT NULL
    
    UNION ALL
    
    -- Recursive Step: Walk down the dependency tree
    SELECT
        dd.dag_id,
        (x.trigger_id)::varchar,
        'dataset'::varchar,
        concat(trigger_id, '_lvl_', lvl + 1)::varchar,
        'dataset'::varchar,
        json_array_elements(dataset_dependencies -> 'objects'),
        'all'::varchar,
        lvl + 1,
        (json_array_elements(dataset_dependencies -> 'objects') ->> 'uri')
    FROM datasets_dependencies dd
    JOIN LATERAL (SELECT dd.trigger_id, dd.trigger_type) x ON true
    WHERE dataset_dependencies -> 'objects' IS NOT NULL
)
-- 3. Final selection joining Metadata
SELECT 
    dd.dep_id,
    dd.dep_type,
    dd.trigger_id,
    dd.trigger_type,
    COALESCE(dur.mean_duration, interval '5 minutes') as dep_mean_duration,
    d.is_paused as dep_is_paused,
    d.owners as deps_owners,
    dd.lvl as depth
FROM datasets_dependencies dd
JOIN dag d ON dd.dag_id = d.dag_id
LEFT JOIN dags_durations dur ON dd.dag_id = dur.dag_id
WHERE d.is_active = true;