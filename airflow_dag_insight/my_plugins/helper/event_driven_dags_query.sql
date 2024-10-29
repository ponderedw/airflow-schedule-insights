 with recursive
        dags_runs as (
        select
            *,
            row_number() over(partition by dag_id
        order by
            start_date desc) row_id
        from
            dag_run
        where
            state = 'success'
                                ),
                                dags_durations as (
        select
            dag.dag_id,
            dag.owners,
            percentile_cont(0.5) within group (
        order by
            coalesce(dr.end_date - dr.start_date,
            interval '5 minutes')) mean_duartion,
            case
                when dataset_expression::text like '%all%' then 'all'
                else 'any'
            end cond_trigger,
            schedule_interval,
            coalesce(dag.next_dagrun_create_after,
            dag.next_dagrun_data_interval_end) as next_dag_run,
            dag.is_paused,
            dag.is_active
        from
            dag
        left join dags_runs dr on
            dr.dag_id = dag.dag_id
            and row_id <= 30
        group by
            dag.dag_id),
        datasets_dependencies(
        dag_id,
        dep_id,
        dep_type,
        trigger_id,
        TRIGGER_TYPE,
        dataset_dependencies,
        condition_type,
        lvl,
        dataset) as (
        select
            cast(dag_id as varchar) as dag_id,
            cast(dag_id as varchar) as dep_id,
            'DAG' DEP_TYPE,
            concat(cast(dag_id as varchar),
            '_',
            "data" -> 'dag' -> 'timetable' -> '__var' -> 'dataset_condition' ->> '__type',
            '_1') as trigger_id,
            "data" -> 'dag' -> 'timetable' -> '__var' -> 'dataset_condition' ->> '__type' TRIGGER_TYPE,
            "data" -> 'dag' -> 'timetable' -> '__var' -> 'dataset_condition' dataset_dependencies,
            'any' condition_type,
            1 lvl,
            null dataset
        from
            serialized_dag sd
        where
            "data" -> 'dag' -> 'timetable' -> '__var' -> 'dataset_condition' is not null
        union all
        select
            cast(dag_id as varchar) as dag_id,
            cast(x.trigger_id as varchar) as dep_id,
            x.TRIGGER_TYPE as DEP_TYPE,
            concat(trigger_id,
            '_',
            json_array_elements(dataset_dependencies -> 'objects')->>'__type',
            '_lvl_',
            lvl + 1,
            '_',
            row_number() over(partition by cast(x.trigger_id as varchar))) trigger_id,
            json_array_elements(dataset_dependencies -> 'objects')->>'__type' TRIGGER_TYPE,
            json_array_elements(dataset_dependencies -> 'objects') dataset_dependencies,
            case
                when dataset_dependencies ->> '__type' = 'dataset_all' then 'all'
                else 'any'
            end condition_type,
            lvl + 1 as lvl,
            json_array_elements(dataset_dependencies -> 'objects')->>'uri' as dataset
        from
            datasets_dependencies x
        where
            dataset_dependencies -> 'objects' is not null

        ),
        dependencies as (
        select
            dag_id,
            json_array_elements(data -> 'dag' -> 'dag_dependencies') ->> 'dependency_id' as dependency_id,
            json_array_elements(data -> 'dag' -> 'dag_dependencies') ->> 'source' source,
            json_array_elements(data -> 'dag' -> 'dag_dependencies') ->> 'target' target,
            json_array_elements(data -> 'dag' -> 'dag_dependencies') ->> 'dependency_type' dep_type
        from
            serialized_dag),
        already_tiggered_datasets as (
        select
                d.uri ,
                dq.target_dag_id
        from
                dataset_dag_run_queue dq,
                dataset d
        where
                dq.dataset_id = d.id
        ),
        datasets_triggers as (
        select
            dag_id,
            dependency_id dataset
        from
            dependencies
        where
            "source" = dag_id
            and dep_type = 'dataset'),
        all_dependencies as (
        select
            dag_id,
            dep_id,
            dep_type,
            case
                when trigger_type = 'dataset' then dataset
                else trigger_id
            end trigger_id,
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
        select
            distinct
            null dag_id,
            dd.dataset as "dep_id",
            trigger_type as "dep_type",
            dt.dag_id trigger_id,
            'DAG' trigger_type,
            'any' condition_type
        from
            datasets_dependencies dd,
            datasets_triggers dt
        where
            dt.dataset = dd.dataset
            and trigger_type = 'dataset'
        union all
        select
            distinct
            target dag_id,
            target as dep_id,
            'DAG' "dep_type",
            source "trigger_id",
            'DAG' trigger_type,
            'any' condition_type
        from
            dependencies
        where
            dep_type = 'trigger' ),
        dags_next_run as (
        select
            cast(dd.dag_id as varchar) trigger_id,
            'DAG' trigger_type,
            least(
            case
                when start_date + dd.mean_duartion < current_timestamp then current_timestamp
                else start_date + dd.mean_duartion
            end,
            case
                when dd.next_dag_run + dd.mean_duartion < current_timestamp then current_timestamp
                else dd.next_dag_run + dd.mean_duartion
            end) end_date
        from
            dags_durations dd
        left join dag_run dr on
            state = 'running'
            and dr.dag_id = dd.dag_id
        where
        --	least(
        --	case
        --		when start_date + dd.mean_duartion < current_timestamp then current_timestamp
        --		else start_date + dd.mean_duartion
        --	end,
        --	dd.next_dag_run) is not null
        --		and
                schedule_interval <> 'null' and dd.is_paused is False
        ),
        scheduled_dags as (
            select
                dag_id
            from
                serialized_dag x
            where
                x."data" -> 'dag' -> 'timetable' ->> '__type' in ('airflow.timetables.interval.CronDataIntervalTimetable', 'airflow.timetables.datasets.DatasetOrTimeSchedule')
        ),
        all_dependencies_enriched as (
        select
            ad.*,
            coalesce(dd.mean_duartion,
            interval '0 minutes') dep_mean_duration,
            dd.is_paused dep_is_paused,
            dd.is_active dep_is_active,
            coalesce(dd_triggers.mean_duartion,
            interval '0 minutes') trigger_mean_duration,
            dd_triggers.is_paused trigger_is_paused,
            dd_triggers.is_active trigger_is_active,
            dnr.end_date trigger_end_date,
            dd.owners deps_owners,
            case when sd.dag_id is Null then false else true end ind_dep_scheduled
        from
            all_dependencies ad
        left join dags_durations dd on
            ad.dep_type = 'DAG' and ad.dep_id = dd.dag_id
        left join dags_durations dd_triggers on
            ad.trigger_type = 'DAG' and ad.trigger_id = dd_triggers.dag_id
        left join dags_next_run dnr on
            ad.trigger_id = dnr.trigger_id
            and ad.trigger_type = dnr.trigger_type
        left join scheduled_dags sd on
            sd.dag_id = ad.dep_id and ad.dep_type = 'DAG'
        where
            (ad.dag_id, ad.trigger_id) not in (
            select
                x.target_dag_id, x.uri
            from
                already_tiggered_datasets x) or ad.dep_type <> 'dataset_all' or ad.trigger_type <> 'dataset'
            )

        select *,
            case
                when dep_id not in (select distinct x.trigger_id from all_dependencies_enriched x)
                    then true else false end ind_root,
            case when trigger_id  not in (select distinct x.dep_id from all_dependencies_enriched x)
                then true else false end ind_leaf
        from  all_dependencies_enriched

        union all

        select
            dag_id,
            dag_id dep_id,
            'DAG' dep_type,
            null trigger_id,
            null trigger_type,
            null condition_type,
            null dep_mean_duration,
            is_paused dep_is_paused,
            is_active dep_is_active,
            null trigger_mean_duration,
            null trigger_is_paused,
            null trigger_is_active,
            null trigger_end_date,
            owners deps_owners,
            null ind_scheduled,
            null ind_root,
            null ind_leaf
        from
            dag
        where
            (schedule_interval = 'null'
                or is_paused = true)
            and is_active = true
            and dag_id not in (
            select
                distinct x.dep_id
            from
                all_dependencies_enriched x)