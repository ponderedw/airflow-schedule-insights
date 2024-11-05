with recursive
dags_runs as (
    select
        *,
        row_number() over (
            partition by dag_id
            order by
                start_date desc
        ) as row_id
    from
        dag_run
    where
        state = 'success'
),

dags_durations as (
    select
        dag.dag_id,
        dag.owners,
        dag.schedule_interval,
        dag.is_paused,
        dag.is_active,
        percentile_cont(0.5) within group (
            order by
                coalesce(
                    dr.end_date - dr.start_date,
                    interval '5 minutes'
                )
        ) as mean_duartion,
        case
            when dag.dataset_expression::text like '%all%' then 'all'
            else 'any'
        end as cond_trigger,
        coalesce(
            dag.next_dagrun_create_after,
            dag.next_dagrun_data_interval_end
        ) as next_dag_run
    from
        dag
    left join dags_runs as dr
        on
            dag.dag_id = dr.dag_id
            and dr.row_id <= 30
    group by
        dag.dag_id
),

datasets_dependencies (
    dag_id,
    dep_id,
    dep_type,
    trigger_id,
    trigger_type,
    dataset_dependencies,
    condition_type,
    lvl,
    dataset) as (
    select
        dag_id::varchar as dag_id,
        dag_id::varchar as dep_id,
        'DAG' as dep_type,
        concat(
            dag_id::varchar,
            '_',
            "data"
            -> 'dag'
            -> 'timetable'
            -> '__var'
            -> 'dataset_condition'
            ->> '__type',
            '_1'
        ) as trigger_id,
        "data"
        -> 'dag'
        -> 'timetable'
        -> '__var'
        -> 'dataset_condition'
        ->> '__type' as trigger_type,
        "data"
        -> 'dag'
        -> 'timetable'
        -> '__var'
        -> 'dataset_condition' as dataset_dependencies,
        'any' as condition_type,
        1 as lvl,
        "data"
        -> 'dag'
        -> 'timetable'
        -> '__var'
        -> 'dataset_condition'
        ->> 'uri' as dataset
    from
        serialized_dag
    where
        "data"
        -> 'dag'
        -> 'timetable'
        -> '__var'
        -> 'dataset_condition' is not null
    union all
    select
        dag_id::varchar as dag_id,
        (x.trigger_id)::varchar as dep_id,
        x.trigger_type as dep_type,
        concat(
            trigger_id,
            '_',
            json_array_elements(dataset_dependencies -> 'objects') ->> '__type',
            '_lvl_',
            lvl + 1,
            '_',
            row_number() over (partition by (x.trigger_id)::varchar)
        ) as trigger_id,
        json_array_elements(dataset_dependencies -> 'objects')
        ->> '__type' as trigger_type,
        json_array_elements(
            dataset_dependencies -> 'objects'
        ) as dataset_dependencies,
        case
            when dataset_dependencies ->> '__type' = 'dataset_all' then 'all'
            else 'any'
        end as condition_type,
        lvl + 1 as lvl,
        json_array_elements(dataset_dependencies -> 'objects')
        ->> 'uri' as dataset
    from
        datasets_dependencies as x
    where
        dataset_dependencies -> 'objects' is not null

),

"dependencies" as (
    select
        dag_id,
        json_array_elements("data" -> 'dag' -> 'dag_dependencies')
        ->> 'dependency_id' as dependency_id,
        json_array_elements("data" -> 'dag' -> 'dag_dependencies')
        ->> 'source' as source,
        json_array_elements("data" -> 'dag' -> 'dag_dependencies')
        ->> 'target' as target,
        json_array_elements("data" -> 'dag' -> 'dag_dependencies')
        ->> 'dependency_type' as dep_type
    from
        serialized_dag
),

already_tiggered_datasets as (
    select
        d.uri,
        dq.target_dag_id
    from
        dataset_dag_run_queue as dq,
        dataset as d
    where
        dq.dataset_id = d.id
),

datasets_triggers as (
    select
        dag_id,
        dependency_id as dataset
    from
        "dependencies"
    where
        source = dag_id
        and dep_type = 'dataset'
),

all_dependencies as (
    select
        dag_id,
        dep_id,
        dep_type,
        case
            when trigger_type = 'dataset' then dataset
            else trigger_id
        end as trigger_id,
        trigger_type,
        condition_type
    from
        datasets_dependencies
        --where
        --	dataset not in (
        --	select
        --		x.uri
        --	from
        --		already_tiggered_datasets x) or dep_type <> 'dataset_all'
    union all
    select distinct
        null as dag_id,
        dd.dataset as dep_id,
        trigger_type as dep_type,
        dt.dag_id as trigger_id,
        'DAG' as trigger_type,
        'any' as condition_type
    from
        datasets_dependencies as dd,
        datasets_triggers as dt
    where
        dt.dataset = dd.dataset
        and trigger_type = 'dataset'
    union all
    select distinct
        target as dag_id,
        target as dep_id,
        'DAG' as dep_type,
        source as trigger_id,
        'DAG' as trigger_type,
        'any' as condition_type
    from
        "dependencies"
    where
        dep_type = 'trigger'
),

dags_next_run as (
    select
        'DAG' as trigger_type,
        (dd.dag_id)::varchar as trigger_id,
        least(
            dr.start_date,
            case
                when dd.next_dag_run < current_timestamp then current_timestamp
                else dd.next_dag_run
            end
        ) as start_date,
        least(
            case
                when
                    dr.start_date + dd.mean_duartion < current_timestamp
                    then current_timestamp
                else dr.start_date + dd.mean_duartion
            end,
            case
                when
                    dd.next_dag_run + dd.mean_duartion < current_timestamp
                    then current_timestamp
                else dd.next_dag_run + dd.mean_duartion
            end
        ) as end_date
    from
        dags_durations as dd
    left join dag_run as dr
        on
            dr.state = 'running'
            and dd.dag_id = dr.dag_id
    where
        dd.schedule_interval <> 'null' and dd.is_paused is false
),

scheduled_dags as (
    select x.dag_id
    from
        serialized_dag as x
    where
        x."data"
        -> 'dag'
        -> 'timetable'
        ->> '__type' in (
            'airflow.timetables.interval.CronDataIntervalTimetable',
            'airflow.timetables.datasets.DatasetOrTimeSchedule'
        )
),

triggers_event_durations as (
    select
        d.target as dep_id,
        'DAG' as dep_type,
        d.source as trigger_id,
        'DAG' as trigger_type,
        least(percentile_cont(0.5) within group (order by coalesce(
            x.start_date - y.start_date,
            interval '5 minutes'
        )), dd.mean_duartion) as duration
    from
        task_instance as x,
        dags_runs as y,
        "dependencies" as d,
        dags_durations as dd
    where
        d.dep_type = 'trigger'
        and d.source = d.dag_id
        and y.row_id <= 30
        and y.run_id = x.run_id
        and y.dag_id = x.dag_id
        and x.task_id = d.dependency_id
        and x.dag_id = d.dag_id
        and x.state = 'success'
        and dd.dag_id = y.dag_id
    group by d.source, d.target, dd.mean_duartion

    union all

    select
        d.uri as dep_id,
        'dataset' as dep_type,
        de.source_dag_id as trigger_id,
        'DAG' as trigger_type,
        least(percentile_cont(0.5) within group (order by coalesce(
            de."timestamp" - y.start_date,
            interval '5 minutes'
        )), dd.mean_duartion) as duration
    from
        dataset_event as de,
        dataset as d,
        dags_runs as y,
        "dependencies" as dep,
        dags_durations as dd
    where
        dep.dep_type = 'dataset'
        and dep.source = dep.dag_id
        and y.row_id <= 30
        and y.run_id = de.source_run_id
        and y.dag_id = de.source_dag_id
        and de.dataset_id = d.id
        and dep.dependency_id = d.uri
        and dd.dag_id = y.dag_id
    group by de.source_dag_id, d.uri, dd.mean_duartion

),

all_dependencies_enriched as (
    select
        ad.*,
        coalesce(
            dd.mean_duartion,
            interval '0 minutes'
        ) as dep_mean_duration,
        dd.is_paused as dep_is_paused,
        dd.is_active as dep_is_active,
        coalesce(
            dd_triggers.mean_duartion,
            interval '0 minutes'
        ) as trigger_mean_duration,
        coalesce(
            ted.duration,
            coalesce(
                dd_triggers.mean_duartion,
                interval '0 minutes'
            )
        ) as trigger_event_mean_duration,
        dd_triggers.is_paused as trigger_is_paused,
        dd_triggers.is_active as trigger_is_active,
        case
            when
                coalesce(dnr.start_date + ted.duration, dnr.end_date) is null
                then null
            else dnr.start_date
        end as trigger_start_date,
        case
            when
                coalesce(dnr.start_date + ted.duration, dnr.end_date) is null
                then null
            when
                coalesce(dnr.start_date + ted.duration, dnr.end_date)
                < current_timestamp
                then current_timestamp
            else coalesce(dnr.start_date + ted.duration, dnr.end_date)
        end as trigger_end_date,
        ted.duration as trigger_ted_duration,
        dnr.end_date as dag_trigger_end_date,
        dd.owners as deps_owners,
        not coalesce(sd.dag_id is null, false) as ind_dep_scheduled
    from
        all_dependencies as ad
    left join dags_durations as dd
        on
            ad.dep_type = 'DAG' and ad.dep_id = dd.dag_id
    left join dags_durations as dd_triggers
        on
            ad.trigger_type = 'DAG' and ad.trigger_id = dd_triggers.dag_id
    left join dags_next_run as dnr
        on
            ad.trigger_id = dnr.trigger_id
            and ad.trigger_type = dnr.trigger_type
    left join triggers_event_durations as ted
        on
            ad.trigger_id = ted.trigger_id
            and ad.trigger_type = ted.trigger_type
            and ad.dep_id = ted.dep_id
            and ad.dep_type = ted.dep_type
    left join scheduled_dags as sd
        on
            ad.dep_id = sd.dag_id and ad.dep_type = 'DAG'
    where
        (ad.dag_id, ad.trigger_id) not in (
            select
                x.target_dag_id,
                x.uri
            from
                already_tiggered_datasets as x
        ) or ad.dep_type <> 'dataset_all' or ad.trigger_type <> 'dataset'
)

select
    *,
    coalesce(
        dep_id not in (
            select distinct x.trigger_id from all_dependencies_enriched as x
        ), false
    ) as ind_root,
    coalesce(trigger_id not in (
        select distinct x.dep_id from all_dependencies_enriched as x
    ),
    false) as ind_leaf
from all_dependencies_enriched

union all

select
    dag_id,
    dag_id as dep_id,
    'DAG' as dep_type,
    null as trigger_id,
    null as trigger_type,
    null as condition_type,
    null as dep_mean_duration,
    is_paused as dep_is_paused,
    is_active as dep_is_active,
    null as trigger_mean_duration,
    null as trigger_event_mean_duration,
    null as trigger_is_paused,
    null as trigger_is_active,
    null as trigger_start_date,
    null as trigger_end_date,
    null as trigger_ted_duration,
    null as dag_trigger_end_date,
    owners as deps_owners,
    null as ind_scheduled,
    null as ind_root,
    null as ind_leaf
from
    dag
where
    (
        schedule_interval = 'null'
        or is_paused = true
    )
    and is_active = true
    and dag_id not in (
        select distinct x.dep_id
        from
            all_dependencies_enriched as x
    )
