from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request, redirect, url_for
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from airflow import settings
from airflow.models import DagRun, DagModel
from datetime import datetime, timedelta, timezone
import pytz
from sqlalchemy import String, text
from sqlalchemy.sql import and_, func, select
from sqlalchemy.sql.functions import coalesce
from croniter import croniter, CroniterBadCronError
import pandas as pd
import pathlib


bp = Blueprint(
    'dag_insight',
    __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/dag_insight_plugin',
)
session = settings.Session()


class DagInsightAppBuilderBaseView(AppBuilderBaseView):
    default_view = 'main'
    route_base = '/dag_insight'

    def __init__(self):
        super().__init__()
        self.records = []
        self.future_runs = []
        self.not_running_dags = []

    def is_valid_cron(self, cron_string):
        try:
            croniter(cron_string)  # Attempt to create a croniter object
            return True
        except CroniterBadCronError:
            return False

    def get_valid_cron(self, schedule_interval):
        for cron_string in schedule_interval.split(' or '):
            if self.is_valid_cron(cron_string):
                return cron_string
        return None

    def predict_future_cron_runs(self, cron_schedule, start_dt, end_dt, next_dagrun, max_runs=100):
        # Initialize croniter with the cron schedule and start date
        cron = croniter(cron_schedule, next_dagrun)
        start_dt = max(datetime.now(timezone.utc), start_dt)
        future_runs = []
        count = 0
        # Generate future runs between start_dt and end_dt
        while count < max_runs:
            next_run = cron.get_next(datetime)  # Get the next run as a datetime
            if next_run > end_dt:
                break  # Stop if the next run is beyond the end date
            if next_run < start_dt:
                continue
            future_runs.append(next_run)
            count += 1

        return future_runs

    def get_next_cron_run(self, cron_schedule):
        current_time = datetime.now(timezone.utc)
        iter = croniter(cron_schedule, current_time)
        return iter.get_next(datetime)

    def is_naive(self, dt):
        return dt.tzinfo is None

    def handle_datetime_string(self, dt_string, default_string, client_timezone):
        dt_string_cleaned = default_string if dt_string == '' or dt_string is None else dt_string
        dt = datetime.fromisoformat(dt_string_cleaned)
        if client_timezone is not None and client_timezone != 'None' and self.is_naive(dt):
            client_timezone = pytz.timezone(client_timezone)
            dt = client_timezone.localize(dt)
        return dt

    def get_filter_dates(self, start, end, client_timezone):
        start_of_time = '2000-01-01T14:33+00:00'
        end_of_time_dt = datetime.now(timezone.utc) + timedelta(hours=4)
        end_of_time = end_of_time_dt.isoformat()
        start_dt = self.handle_datetime_string(start, start_of_time, client_timezone)
        end_dt = self.handle_datetime_string(end, end_of_time, client_timezone)
        return start_dt, end_dt, end_of_time_dt

    def get_fixed_task_data(self):
        return [
            {'dag_id': 'task_1', 'start_time': '2024-09-25T10:00:00', 'end_time': '2024-09-25T10:30:00',
             'state': 'succeeded', 'manual_run': False},
            {'dag_id': 'task_2', 'start_time': '2024-09-25T10:15:00', 'end_time': '2024-09-25T10:45:00',
             'state': 'running', 'manual_run': True}
        ]

    def get_dependency_end_time(self, dep, start_dt, end_dt):
        if dep['ind_leaf']:
            start_time = dep['trigger_end_date']  # Take next run of scheduled DAG
            path = dep['trigger_id'] if dep['trigger_type'] == 'DAG' else ''
        else:
            start_time, path = self.calculate_events_end_dates(dep['trigger_id'],
                                                               dep['trigger_type'], start_dt, end_dt)
            if start_time is None:
                start_time = dep['trigger_end_date']
                if dep['trigger_end_date']:
                    path = dep['trigger_id'] if dep['trigger_type'] == 'DAG' else ''
            elif dep['trigger_end_date'] is not None:
                if dep['trigger_end_date'] < start_time:
                    path = dep['trigger_id'] if dep['trigger_type'] == 'DAG' else ''
                start_time = min(dep['trigger_end_date'], start_time)
        return (start_time, path)

    def get_final_start_time(self, deps, condition_type, start_dt, end_dt):
        all_is_wrong = False
        failed_path = ''
        start_times = []
        for dep in deps:
            start_time, path = self.get_dependency_end_time(dep, start_dt, end_dt)
            if start_time is not None:
                start_times.append({'start_time': start_time,
                                    'run_type': 'trigger' if dep['dep_type'] == 'DAG' and dep['trigger_type'] == 'DAG'
                                    else 'dataset',
                                    'path': path + ' -> ' + dep['dep_id'] if dep['dep_type'] == 'DAG' else path})
            else:
                failed_path = path + ' -> ' + dep['dep_id'] if dep['dep_type'] == 'DAG' else path
                if condition_type == 'all':
                    all_is_wrong = True
                    break
        if condition_type == 'any':
            if len(start_times) == 0:
                description = "DAG won't run because it won't be triggered by any of its dependencies. "
                description += failed_path
                final_start_time = {'start_time': None, 'description': description, 'path': failed_path}
            else:
                final_start_time = min(start_times, key=lambda x: x['start_time'])
        elif condition_type == 'all':
            if all_is_wrong:
                description = "DAG won't run because it won't be triggered by at least one of its dependencies. "
                description += failed_path
                final_start_time = {'start_time': None, 'description': description, 'path': failed_path}
            else:
                final_start_time = max(start_times, key=lambda x: x['start_time'])
        return final_start_time

    def get_deps_data(self, deps):
        first_dep = deps[0]
        owners = first_dep['deps_owners']
        condition_type = first_dep['condition_type']
        dep_mean_duration = first_dep['dep_mean_duration']
        dep_is_paused = first_dep['dep_is_paused']
        ind_scheduled = first_dep['ind_dep_scheduled']
        return (owners, condition_type, dep_mean_duration, dep_is_paused, ind_scheduled)

    def update_future_missing_dags(self, dag_id, final_start_time):
        self.not_running_dags.append({'dag_id': dag_id,
                                      'description': final_start_time.get('description'),
                                      'path': final_start_time.get('path')})

    def update_future_runs(self, dag_id, final_start_time, final_end_time, owners, path, dep_mean_duration):
        self.future_runs.append({'dag_id': dag_id,
                                 'start_time': final_start_time.get('start_time'),
                                 'end_time': final_end_time,
                                 'state': 'forecast',
                                 'owner': owners,
                                 'schedule_interval': path,
                                 'run_type': final_start_time.get('run_type'),
                                 'duration': str(dep_mean_duration).split('.')[0]})

    def update_future_metadata(self, final_start_time, dep_id, final_end_time, start_dt, end_dt,
                               owners, path, dep_mean_duration, ind_scheduled):
        if final_start_time.get('start_time') is None \
                and dep_id not in [dag['dag_id'] for dag in self.not_running_dags] \
                and ind_scheduled is False:
            self.update_future_missing_dags(dep_id, final_start_time)
        elif final_start_time.get('start_time') is not None \
                and dep_id not in [dag['dag_id'] for dag in self.future_runs] \
                and final_start_time.get('start_time') <= end_dt \
                and final_end_time >= start_dt:
            self.update_future_runs(dep_id, final_start_time, final_end_time, owners, path, dep_mean_duration)

    def calculate_events_end_dates(self, dep_id, dep_type, start_dt, end_dt):
        deps = [record for record in self.records if record['dep_id'] == dep_id and record['dep_type'] == dep_type]
        if len(deps) == 0:
            return (None, '')  # if Dataset was triggered but the DAG is paused
        owners, condition_type, dep_mean_duration, dep_is_paused, ind_scheduled = self.get_deps_data(deps)
        final_start_time = self.get_final_start_time(deps, condition_type, start_dt, end_dt)
        if dep_is_paused and dep_type == 'DAG':  # Dag won't run because it's paused
            final_start_time = {'start_time': None, 'description': 'The DAG is paused', 'path': dep_id}
        final_end_time = None if final_start_time.get('start_time') is None \
            else final_start_time['start_time'] + dep_mean_duration
        path = final_start_time.get('path', '')
        if dep_type == 'DAG':
            self.update_future_metadata(final_start_time, dep_id, final_end_time, start_dt, end_dt,
                                        owners, path, dep_mean_duration, ind_scheduled)
        return (final_end_time, path)

    def get_future_dependencies_runs(self, start_dt, end_dt):
        roots = list(set([(record['dep_id'], record['dep_type']) for record in self.records if record['ind_root']]))
        for root in roots:
            self.calculate_events_end_dates(root[0], root[1], start_dt, end_dt)

    def update_event_driven_runs_metadata(self, start_dt: datetime, end_dt: datetime):
        datasets_predictions_query_path = pathlib.Path(__file__).parent.resolve() \
            / 'helper/event_driven_dags_query.sql'
        with open(datasets_predictions_query_path, 'r') as query_f:
            datasets_predictions_query = query_f.read()
        df = pd.read_sql(text(datasets_predictions_query), session.connection())
        df = df.replace({pd.NaT: None})
        self.records = df.to_dict('records')
        self.get_future_dependencies_runs(start_dt, end_dt)
        stopped_leafs = [record for record in self.records if record['ind_root'] is None]
        for leaf in stopped_leafs:
            if leaf['dep_is_paused']:
                description = 'The DAG is paused'
            else:
                description = "DAG doesn't have a schedule or other dependencies"
            self.not_running_dags.append({'dag_id': leaf['dep_id'],
                                          'description': description,
                                          'path': leaf['dep_id']})
        self.not_running_dags = sorted(self.not_running_dags, key=lambda x: x['dag_id'])

    def get_scheduled_dags_meta_query(self):
        base_query = select(DagRun.dag_id,
                            DagRun.start_date,
                            DagRun.end_date,
                            func.row_number().over(
                                partition_by=DagRun.dag_id,
                                order_by=DagRun.start_date.desc()
                            ).label('row_num')
                            ).where(and_(
                                DagRun.state == 'success')).alias('base')
        query = select(
                    DagModel.dag_id,
                    DagModel.next_dagrun_data_interval_end,
                    DagModel.owners,
                    DagModel.timetable_description,
                    coalesce(
                        coalesce(DagModel.next_dagrun, DagModel.next_dagrun_data_interval_start),
                        DagModel.next_dagrun_data_interval_end).label('next_dagrun'),
                    func.replace(DagModel.schedule_interval, '"', '').label('schedule_interval'),
                    coalesce(func.avg(base_query.c.end_date - base_query.c.start_date), text("INTERVAL '5 minutes'"))
                        .label('avg_duration'),
                    coalesce(func.percentile_cont(0.5).within_group(
                        base_query.c.end_date - base_query.c.start_date),
                             text("INTERVAL '5 minutes'")).label('duration')
                ).join(base_query, and_(DagModel.dag_id == base_query.c.dag_id,
                                        base_query.c.row_num <= 30), isouter=True).where(
                    and_(DagModel.schedule_interval.cast(String) != 'null',
                         DagModel.schedule_interval.cast(String) != '"Dataset"',
                         DagModel.is_active.is_(True),
                         DagModel.is_paused.is_(False)
                         )
                ).group_by(DagModel.dag_id, DagModel.next_dagrun_data_interval_end,
                           DagModel.owners, DagModel.timetable_description, DagModel.schedule_interval)
        return query

    def update_predicted_runs(self, start_dt: datetime, end_dt: datetime) -> None:
        self.future_runs = []
        self.not_running_dags = []
        scheduled_dags_meta_query = self.get_scheduled_dags_meta_query()
        compiled_query = scheduled_dags_meta_query.compile(compile_kwargs={'literal_binds': True})
        scheduled_dags = session.execute(str(compiled_query)).all()
        dags_data = []
        for dag in scheduled_dags:
            cron = self.get_valid_cron(dag.schedule_interval)
            if cron:
                future_runs = self.predict_future_cron_runs(cron, start_dt, end_dt, dag.next_dagrun)
                for run in future_runs:
                    dag_info = {
                        'dag_id': dag.dag_id,
                        'start_time': run if
                        run else None,
                        'end_time': (run + dag.duration) if
                        run else None,
                        'state': 'forecast',
                        'owner': dag.owners,  # Fetch owner from conf or use 'unknown'
                        'schedule_interval': dag.timetable_description,
                        'run_type': 'scheduled',
                        'duration': str(dag.duration).split('.')[0]
                    }
                    dags_data.append(dag_info)
        self.update_event_driven_runs_metadata(start_dt, end_dt)
        self.future_runs = dags_data + self.future_runs
        self.future_runs = sorted(self.future_runs, key=lambda x: x['start_time'])

    def get_dags_data(self, start_dt: datetime, end_dt: datetime, show_future_runs: str) -> list:
        dag_runs = session.query(DagRun).filter(and_(coalesce(DagRun.end_date, func.now()) >= start_dt,
                                                     DagRun.start_date <= end_dt)).all()
        dags_data = []
        for run in dag_runs:
            # Fetch the DAG model to get the schedule_interval
            dag_model = session.query(DagModel).filter(DagModel.dag_id == run.dag_id).first()
            # Create the dictionary for this DAG run
            dag_info = {
                'dag_id': run.dag_id,
                'start_time': run.start_date.isoformat() if run.start_date else None,
                'end_time': run.end_date.isoformat() if run.end_date else
                datetime.now(timezone.utc).isoformat(),
                'state': run.state,
                'owner': dag_model.owners,
                'schedule_interval': dag_model.timetable_description if dag_model else datetime.now(timezone.utc),
                'run_type': run.run_type
            }
            dag_info['duration'] = str(datetime.fromisoformat(dag_info['end_time']) -
                                       datetime.fromisoformat(dag_info['start_time'])) \
                .split('.')[0]
            dags_data.append(dag_info)
        if show_future_runs == 'true':
            self.update_predicted_runs(start_dt, end_dt)
            dags_data = dags_data + self.future_runs
        return dags_data

    def get_time_filter(self, start_dt: datetime, end_dt: datetime, start: str,
                        end_of_time_dt: datetime, show_future_runs: str) -> dict:
        time_filter = {}
        time_filter['start'] = start_dt.replace(second=0, microsecond=0).isoformat() \
            if start is not None and start != '' else ''
        time_filter['end'] = end_dt.replace(second=0, microsecond=0).isoformat()
        time_filter['max_end'] = end_of_time_dt.replace(second=0, microsecond=0).isoformat()
        time_filter['show_future_runs'] = show_future_runs
        return time_filter

    def get_params_from_request(self) -> tuple:
        start = request.args.get('start')
        end = request.args.get('end')
        client_timezone = request.args.get('timezone')
        show_future_runs = request.args.get('show_future_runs')
        return (start, end, client_timezone, show_future_runs)

    @expose('/')
    def main(self):
        time_limit = datetime.now(timezone.utc) - timedelta(hours=4)
        return redirect(url_for('DagInsightAppBuilderBaseView.dag_insight', start=time_limit.isoformat()))

    @expose('/dag_insight')
    def dag_insight(self):
        start, end, client_timezone, show_future_runs = self.get_params_from_request()
        start_dt, end_dt, end_of_time_dt = self.get_filter_dates(start, end, client_timezone)
        time_filter = self.get_time_filter(start_dt, end_dt, start, end_of_time_dt, show_future_runs)
        dags_data = self.get_dags_data(start_dt, end_dt, show_future_runs)
        return self.render_template(
            'dag_insight.html',
            task_data=dags_data,
            time_filter=time_filter,
            not_running_dags=self.not_running_dags,
            future_runs=self.future_runs
        )


v_appbuilder_view = DagInsightAppBuilderBaseView()
v_appbuilder_package = {
    'name': 'DAG Insight',
    'category': 'Browse',
    'view': v_appbuilder_view,
}


class DagInsightPlugin(AirflowPlugin):
    name = 'dag_insight'
    hooks = []
    macros = []
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
    appbuilder_menu_items = []
