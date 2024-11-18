from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request, redirect, url_for, jsonify
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from airflow import settings
from airflow.models import DagRun, DagModel
from datetime import datetime, timedelta, timezone
import pytz
from sqlalchemy import text
from sqlalchemy.sql import and_, func, select
from sqlalchemy.sql.functions import coalesce
from croniter import croniter, CroniterBadCronError
import pandas as pd
import pathlib
import numpy as np


bp = Blueprint(
    "schedule_insights",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/airflow_schedule_insights",
)
session = settings.Session()


class ScheduleInsightsAppBuilderBaseView(AppBuilderBaseView):
    default_view = "main"
    route_base = "/schedule_insights"

    def __init__(self):
        super().__init__()
        self.event_driven_dags = []
        self.future_runs = []
        self.next_runs = []
        self.new_schedules = []
        self.selected_dags = []

    def is_valid_cron(self, cron_string):
        try:
            croniter(cron_string)  # Attempt to create a croniter object
            return True
        except CroniterBadCronError:
            return False

    def get_valid_cron(self, schedule_interval):
        for cron_string in schedule_interval.split(" or "):
            if self.is_valid_cron(cron_string):
                return cron_string
        return None

    def predict_future_cron_runs(
        self, cron_schedule, start_dt, end_dt, next_dagrun, max_runs=1000
    ):
        # Initialize croniter with the cron schedule and start date
        if next_dagrun is None:
            next_dagrun = datetime.now(timezone.utc)
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

    def is_datetime_naive(self, dt):
        return dt.tzinfo is None

    def localize_datetime(self, datetime, timezone):
        """Localizes a naive datetime object to the specified timezone.

        This method checks if the provided datetime object is naive
        (i.e., lacking timezone information) and localizes it to
        the specified client timezone if it is not None.

        Parameters:
            datetime (datetime): The naive datetime object to be localized.
            timezone (str): The client's timezone identifier.
                        It should be a valid timezone recognized by the `pytz` library.

        Returns:
            datetime: A timezone-aware datetime object.
                If the original datetime was already timezone-aware
                    or if the timezone is None,
                    the original datetime is returned unchanged.

        Workflow:
            1. Checks if the `timezone` is not None, not the string "None",
            and if the provided `datetime` is naive using `self.is_datetime_naive()`.
            2. If conditions are met, the method retrieves the client timezone using
                `pytz.timezone()`.
            3. The naive datetime object is localized to the specified timezone using
                `localize()`.
            4. The localized datetime object is returned. If the original datetime was
                already timezone-aware, it is returned as is.
        """
        if (
            timezone is not None
            and timezone != "None"
            and self.is_datetime_naive(datetime)
        ):
            client_timezone = pytz.timezone(timezone)
            datetime = client_timezone.localize(datetime)
        return datetime

    def handle_datetime_string(self, dt_string, default_string, client_timezone):
        """Handles and converts a datetime string to a localized datetime object.

        This method takes a user-provided datetime string, checks if it's empty or None,
        and if so, replaces it with a default datetime string. The resulting datetime is
        then localized to the client's timezone.

        Parameters:
            dt_string (str): The datetime string provided by the user.
            default_string (str): A default datetime string to use
                if the user-provided string is empty or None.
            client_timezone (str): The client's timezone identifier.

        Returns:
            datetime: A timezone-aware datetime object localized to
                the specified client timezone.

        Workflow:
            1. Checks if the user-provided `dt_string` is empty or None.
            - If true, assigns `default_string` to `dt_string_cleaned`.
            - Otherwise, assigns `dt_string` directly.
            2. Parses the cleaned datetime string into a naive datetime object using
            `datetime.fromisoformat()`.
            3. Localizes the naive datetime object to the specified `client_timezone`
                using `self.localize_datetime()`.
            4. Returns the localized datetime object.
        """
        dt_string_cleaned = (
            default_string if dt_string == "" or dt_string is None else dt_string
        )
        dt = datetime.fromisoformat(dt_string_cleaned)
        dt = self.localize_datetime(dt, client_timezone)
        return dt

    def get_filter_dates(self, start, end, client_timezone):
        """Generates and converts filter date ranges based on user input and
            client timezone.

        This method takes user-provided start and end date strings, processes them to
        ensure they are in the correct timezone, and generates a default end date based
        on the current time plus a specific offset.

        Parameters:
            start (str): The start date string provided by the user.
            end (str): The end date string provided by the user.
            client_timezone (str): The client's timezone identifier.

        Returns:
            tuple: A tuple containing:
                - start_dt (datetime): The processed start date in UTC.
                - end_dt (datetime): The processed end date in UTC.
                - end_of_time_dt (datetime): The calculated end date based on
                    the current time plus a four-hour offset.

        Workflow:
            1. Sets a fixed start date of "2000-01-01T14:33+00:00" as
                a reference point for processing.
            2. Calculates the end of time by adding four hours to the current UTC time.
            3. Converts the user-provided start date string to a datetime object
                in UTC using `handle_datetime_string`.
            4. Converts the user-provided end date string to
                a datetime object in UTC using `handle_datetime_string`.
            5. Returns the processed start date, end date, and
                the calculated end of time.
        """
        start_of_time = "2000-01-01T14:33+00:00"
        end_of_time_dt = datetime.now(timezone.utc) + timedelta(hours=4)
        end_of_time = end_of_time_dt.isoformat()
        start_dt = self.handle_datetime_string(start, start_of_time, client_timezone)
        end_dt = self.handle_datetime_string(end, end_of_time, client_timezone)
        return start_dt, end_dt, end_of_time_dt

    def get_dependency_end_time(self, dep, start_dt, end_dt):
        """Calculates the end time of a dependency in an event-driven DAG.

        This method retrieves the end time for a specified dependency,
        considering whether it is a leaf node or requires recursive evaluation of
        its dependencies.
        If the dependency is a leaf, it directly uses the trigger end date.
        Otherwise, it calls `calculate_events_end_dates` to
        compute the start time based on its dependencies.

        Parameters:
            dep (dict): A dictionary representing the dependency with keys such as
                        "ind_leaf", "trigger_end_date", "trigger_id", and "trigger_type"
            start_dt (datetime): The start date to consider for future runs.
            end_dt (datetime): The end date to consider for future runs.

        Returns:
            tuple: A tuple containing:
                - start_time (datetime or None): The calculated start time for
                    the dependency, or the trigger end date if applicable.
                - path (str): The path associated with the dependency,
                    or an empty string if not applicable.

        Workflow:
            1. Checks if the dependency is a leaf node.
            2. If it is a leaf, retrieves the trigger end date as the start time.
            3. If not a leaf,
                computes the start time recursively using `calculate_events_end_dates`.
            4. Updates the start time based on the dependency's end date if necessary.
            5. Returns the calculated start time and associated path.
        """
        trigger_id = dep["trigger_id"]
        trigger_end_date = None
        if dep["trigger_start_date"] is not None:
            if dep["trigger_event_mean_duration"] is None:
                trigger_end_date = dep["dag_trigger_end_date"]
            else:
                trigger_end_date = (
                    dep["trigger_start_date"] + dep["trigger_event_mean_duration"]
                )
            if trigger_end_date < datetime.now(timezone.utc):
                trigger_end_date = datetime.now(timezone.utc)
        if dep["ind_leaf"]:
            start_time = trigger_end_date  # Take next run of scheduled DAG
            path = trigger_id if dep["trigger_type"] == "DAG" else ""
        else:
            start_time, path = self.calculate_events_end_dates(
                trigger_id,
                dep["trigger_type"],
                start_dt,
                end_dt,
                dep["trigger_event_mean_duration"],
            )
            if start_time is None:
                start_time = trigger_end_date
                if trigger_end_date:
                    path = trigger_id if dep["trigger_type"] == "DAG" else ""
            elif trigger_end_date is not None:
                if trigger_end_date < start_time:
                    path = trigger_id if dep["trigger_type"] == "DAG" else ""
                start_time = min(trigger_end_date, start_time)
        return (start_time, path)

    def get_final_start_time(self, deps, condition_type, start_dt, end_dt):
        """Determines the final start time for a set of
            dependencies based on their conditions.

        This method calculates the start time of
            a dependency based on its child dependencies
            and the specified condition type (either "any" or "all").
        It handles the logic of finding
            the earliest or latest start time based on the condition type.

        Parameters:
            deps (list): A list of dependency dictionaries to evaluate.
            condition_type (str): The condition type to apply ("any" or "all").
            start_dt (datetime): The start date to consider for future runs.
            end_dt (datetime): The end date to consider for future runs.

        Returns:
            dict: A dictionary containing:
                - "start_time" (datetime or None): The determined start time.
                - "description" (str): A description of why the DAG won't run,
                    if applicable.
                - "path" (str): The path of the dependency chain, if applicable.

        Workflow:
            1. Initializes variables to track start times and failed paths.
            2. Iterates through the dependencies and
                retrieves their start times using `get_dependency_end_time`.
            3. Based on the condition type, determines whether to
                return the earliest or latest start time.
            4. Constructs appropriate descriptions if no valid start times are found.
        """

        all_is_wrong = False
        failed_path = ""
        start_times = []
        missing_description = "The DAG won't run because "
        all_missing_description = (
            missing_description + "it won't be triggered by any of its dependencies. "
        )
        any_missing_description = (
            missing_description
            + "it won't be triggered by at least one of its dependencies. "
        )
        for dep in deps:
            start_time, path = self.get_dependency_end_time(dep, start_dt, end_dt)
            run_type = (
                "trigger"
                if dep["dep_type"] == "DAG" and dep["trigger_type"] == "DAG"
                else "dataset"
            )
            if start_time is not None:
                start_times.append(
                    {
                        "start_time": start_time,
                        "run_type": run_type,
                        "path": f"{path} ({run_type})" + " -> " + dep["dep_id"]
                        if dep["dep_type"] == "DAG"
                        else path,
                    }
                )
            else:
                failed_path = (
                    path + f" ({run_type}) -> " + dep["dep_id"]
                    if dep["dep_type"] == "DAG"
                    else path
                )
                if condition_type == "all":
                    all_is_wrong = True
        if condition_type == "any":
            if len(start_times) == 0:
                description = any_missing_description + failed_path
                final_start_time = {
                    "start_time": None,
                    "description": description,
                    "path": failed_path,
                }
            else:
                final_start_time = min(start_times, key=lambda x: x["start_time"])
        elif condition_type == "all":
            if all_is_wrong:
                description = all_missing_description + failed_path
                final_start_time = {
                    "start_time": None,
                    "description": description,
                    "path": failed_path,
                }
            else:
                final_start_time = max(start_times, key=lambda x: x["start_time"])
        return final_start_time

    def get_deps_data(self, deps):
        """Extracts data from the provided dependencies.

        This method retrieves key information from the dependencies, such as owners,
        condition type, mean duration, paused state, and scheduling status.

        Parameters:
            deps (list): A list of dependency dictionaries from which to extract data.

        Returns:
            tuple: A tuple containing:
                - owners (str): The owners of the dependency.
                - condition_type (str): The condition type of the dependency.
                - dep_mean_duration (timedelta): The mean duration of the dependency.
                - dep_is_paused (bool): Indicates if the dependency is paused.
                - ind_scheduled (bool): Indicates if the dependency is scheduled.
        """

        first_dep = deps[0]
        owners = first_dep["deps_owners"]
        condition_type = first_dep["condition_type"]
        dep_mean_duration = first_dep["dep_mean_duration"]
        dep_is_paused = first_dep["dep_is_paused"]
        ind_scheduled = first_dep["ind_dep_scheduled"]
        return (owners, condition_type, dep_mean_duration, dep_is_paused, ind_scheduled)

    def update_future_missing_dags(self, dag_id, final_start_time, owners):
        """Records missing DAGs that won't run in the future.

        This method appends a record to `not_running_dags` for a DAG that
            is not scheduled to run, along with its final start time information.

        Parameters:
            dag_id (str): The identifier of the DAG that is not running.
            final_start_time (dict): A dictionary containing details about
                the final start time.
        """
        ind_selected_dags = "not_selected_dags"
        for dag in self.selected_dags:
            if (
                " " + dag + " " in final_start_time.get("path")
                or final_start_time.get("path").startswith(dag + " ")
                or final_start_time.get("path").endswith(" " + dag)
                or dag_id == dag
            ):
                ind_selected_dags = "selected_dags"
        self.next_runs.append(
            {
                "dag_id": dag_id,
                "description": final_start_time.get("description"),
                "path": final_start_time.get("path"),
                "owner": owners,
                "ind_selected_dags": ind_selected_dags,
            }
        )

    def update_next_runs_dict(self, row_v):
        row = {
            "dag_id": row_v["dag_id"],
            "start_time": row_v["start_time"],
            "end_time": row_v["end_time"],
            "state": row_v["state"],
            "owner": row_v["owner"],
            "description": row_v["schedule_interval"],
            "run_type": row_v["run_type"],
            "duration": row_v["duration"],
            "ind_selected_dags": row_v["ind_selected_dags"],
        }
        for index, item in enumerate(self.next_runs):
            if item["dag_id"] == row["dag_id"]:
                # Compare start_time values
                if (
                    item.get("start_time") is None
                    or row["start_time"] < item["start_time"]
                ):
                    self.next_runs[index] = row
                return  # Stop once we've found and handled the match
        # Append the new_dict if no matching dag_id was found
        self.next_runs.append(row)

    def update_future_runs(
        self,
        dag_id,
        final_start_time,
        final_end_time,
        owners,
        path,
        dep_mean_duration,
        start_dt,
        end_dt,
    ):
        """Records future runs of a DAG based on calculated start and end times.

        This method appends a record to `future_runs` for a scheduled DAG run,
        including its timing, owner, type, and duration.

        Parameters:
            dag_id (str): The identifier of the DAG being scheduled.
            final_start_time (dict): A dictionary containing
                the calculated start time details.
            final_end_time (datetime): The calculated end time for the DAG run.
            owners (str): The owners of the DAG.
            path (str): The path associated with the DAG.
            dep_mean_duration (timedelta): The mean duration for the DAG run.
        """
        state = "forecast"
        simulator_dags = [dag["dag_name"] for dag in self.new_schedules]
        for dag in simulator_dags:
            if (
                " " + dag + " " in path
                or path.startswith(dag + " ")
                or path.endswith(" " + dag)
            ):
                state = "schedule_simulator"
        ind_selected_dags = "not_selected_dags"
        for dag in self.selected_dags:
            if (
                " " + dag + " " in path
                or path.startswith(dag + " ")
                or path.endswith(" " + dag)
                or path == dag
            ):
                ind_selected_dags = "selected_dags"
        row = {
            "dag_id": dag_id,
            "start_time": final_start_time.get("start_time"),
            "end_time": final_end_time,
            "state": state,
            "owner": owners,
            "schedule_interval": path,
            "run_type": final_start_time.get("run_type"),
            "duration": str(dep_mean_duration).split(".")[0],
            "ind_selected_dags": ind_selected_dags,
        }
        if (
            dag_id not in [dag["dag_id"] for dag in self.future_runs]
            and final_start_time.get("start_time") <= end_dt
            and final_end_time >= start_dt
        ):
            self.future_runs.append(row)
        self.update_next_runs_dict(row)

    def update_future_metadata(
        self,
        final_start_time,
        dep_id,
        final_end_time,
        start_dt,
        end_dt,
        owners,
        path,
        dep_mean_duration,
        ind_scheduled,
    ):
        """Updates the future metadata for a dependency based on its calculated timing.

        This method determines whether to record a missing DAG or a scheduled future run
        based on the calculated start time and scheduling status.

        Parameters:
            final_start_time (dict): A dictionary containing
                the calculated start time details.
            dep_id (str): The identifier of the dependency (DAG or dataset).
            final_end_time (datetime): The calculated end time for the dependency.
            start_dt (datetime): The start date for consideration.
            end_dt (datetime): The end date for consideration.
            owners (str): The owners of the dependency.
            path (str): The path associated with the dependency.
            dep_mean_duration (timedelta): The mean duration of the dependency.
            ind_scheduled (bool): Indicates if the dependency is scheduled.
        """
        if (
            final_start_time.get("start_time") is None
            and dep_id not in [dag["dag_id"] for dag in self.next_runs]
            and ind_scheduled is False
        ):
            self.update_future_missing_dags(dep_id, final_start_time, owners)
        elif final_start_time.get("start_time") is not None:
            self.update_future_runs(
                dep_id,
                final_start_time,
                final_end_time,
                owners,
                path,
                dep_mean_duration,
                start_dt,
                end_dt,
            )

    def calculate_events_end_dates(
        self, dep_id, dep_type, start_dt, end_dt, trigger_event_mean_duration
    ):
        """Calculates the end dates for events associated with a dependency.

        This method retrieves dependencies based on the provided dependency ID and type,
        calculates the final start time based on their conditions, and computes the end
        time for both the dependency and the trigger event mean duration.
        It also updates future metadata if the dependency is of type DAG.

        Parameters:
            dep_id (str): The identifier of the dependency for which to calculate
                end dates.
            dep_type (str): The type of the dependency (e.g., "DAG" or "dataset").
            start_dt (datetime): The start date for the calculation window.
            end_dt (datetime): The end date for the calculation window.
            trigger_event_mean_duration (timedelta): The mean duration of
                the triggering event.

        Returns:
            tuple: A tuple containing:
                - trigger_event_mean_duration (datetime or None): The calculated mean
                    duration of the triggering event, or None if not applicable.
                - path (str): The path associated with the dependency.

        Workflow:
            1. Filters `event_driven_dags` to find dependencies matching
                the provided ID and type.
            2. Returns None and an empty string if no matching dependencies are found.
            3. Extracts key data from the dependencies using `get_deps_data`.
            4. Determines the final start time for
                the dependencies using `get_final_start_time`.
            5. If the dependency is a paused DAG, sets the final start time
                to None and provides a corresponding description.
            6. Calculates the final end time based on the determined start time and
                the mean duration.
            7. Updates the triggering event mean duration based on the start time.
            8. If the dependency is a DAG, calls `update_future_metadata`
                to log relevant information.
            9. Returns the calculated mean duration and the path for the dependency.
        """
        deps = [
            record
            for record in self.event_driven_dags
            if record["dep_id"] == dep_id and record["dep_type"] == dep_type
        ]
        if len(deps) == 0:
            return (None, "")  # if Dataset was triggered but the DAG is paused
        # or the node is a leaf
        (
            owners,
            condition_type,
            dep_mean_duration,
            dep_is_paused,
            ind_scheduled,
        ) = self.get_deps_data(deps)
        final_start_time = self.get_final_start_time(
            deps, condition_type, start_dt, end_dt
        )
        if dep_is_paused and dep_type == "DAG":  # Dag won't run because it's paused
            ind_selected_dags = "not_selected_dags"
            if dep_id in self.selected_dags:
                ind_selected_dags = "selected_dags"
            final_start_time = {
                "start_time": None,
                "description": "The DAG is paused",
                "path": dep_id,
                "ind_selected_dags": ind_selected_dags,
            }
        final_end_time = (
            None
            if final_start_time.get("start_time") is None
            else final_start_time["start_time"] + dep_mean_duration
        )
        trigger_event_end_date = (
            None
            if final_start_time.get("start_time") is None
            or trigger_event_mean_duration is None
            else final_start_time["start_time"] + trigger_event_mean_duration
        )
        path = final_start_time.get("path", "")
        if dep_type == "DAG":
            self.update_future_metadata(
                final_start_time,
                dep_id,
                final_end_time,
                start_dt,
                end_dt,
                owners,
                path,
                dep_mean_duration,
                ind_scheduled,
            )
        return (trigger_event_end_date, path)

    def get_future_dependencies_runs(self, start_dt, end_dt):
        """Calculates future runs for event-driven DAGs based on their dependencies.

        This method identifies the unique roots (independent nodes) of event-driven DAGs
        and initiates the calculation of their future runs by calling the
        `calculate_events_end_dates` method, which handles recursion to process
        the entire dependency tree.

        Parameters:
            start_dt (datetime): The start date for filtering future runs.
            end_dt (datetime): The end date for filtering future runs.

        Workflow:
            1. Extracts unique roots from `event_driven_dags` that have an `ind_root`.
            2. For each root, invokes `calculate_events_end_dates` to compute future
            event end dates,
            effectively starting the recursive processing of dependencies.

        Notes:
            - This method relies on the `event_driven_dags` attribute, which should be
            populated prior to calling this method.
            - The `calculate_events_end_dates` method is recursive and handles
            subsequent levels of dependencies.

        Returns:
            None
        """
        roots = list(
            set(
                [
                    (record["dep_id"], record["dep_type"])
                    for record in self.event_driven_dags
                    if record["ind_root"]
                ]
            )
        )
        for root in roots:
            self.calculate_events_end_dates(root[0], root[1], start_dt, end_dt, None)

    def update_event_driven_dags(self):
        """Updates `event_driven_dags` with data from
            a SQL query stored in an external file.

        This method reads a SQL query from the `event_driven_dags_query.sql` file,
        executes it against the current session's database connection, and
        loads the results into `event_driven_dags` as a list of dictionaries.

        Workflow:
            1. Reads the SQL query from `event_driven_dags_query.sql`
                in the helper directory.
            2. Executes the SQL query using SQLAlchemy
                and loads the results into a Pandas DataFrame.
            3. Replaces any `NaT` values with `None` for compatibility.
            4. Converts the DataFrame to a list
                of dictionary records and assigns it to `event_driven_dags`.

        Attributes:
            event_driven_dags (list): A list of dictionaries
                where each dictionary represents a row of
                the query result, storing metadata for event-driven DAGs.
        """
        datasets_predictions_query_path = (
            pathlib.Path(__file__).parent.resolve()
            / "helper/event_driven_dags_query.sql"
        )
        with open(datasets_predictions_query_path, "r") as query_f:
            datasets_predictions_query = query_f.read()
        df = pd.read_sql(text(datasets_predictions_query), session.connection())
        if len(self.new_schedules) > 0:
            changed_schedules_df = pd.DataFrame(self.new_schedules)
            df = df.merge(
                changed_schedules_df,
                left_on="trigger_id",
                right_on="dag_name",
                how="left",
            )
            df["next_custom_run_date"] = np.where(
                df["trigger_type"] == "DAG", df["next_custom_run_date"], np.nan
            )
            df["trigger_start_date"] = df["next_custom_run_date"].combine_first(
                df["trigger_start_date"]
            )
        df = df.replace({pd.NaT: None})
        self.event_driven_dags = df.to_dict("records")

    def append_missing_future_independent_nodes_runs(self):
        """Identifies and appends independent DAGs nodes that
            lack future runs to `not_running_dags`.

        This method inspects the `event_driven_dags` attribute
            to locate DAGs that aren't considered as "leaf nodes" or "root nodes".
        For each such DAG, it checks if the DAG is paused
            or lacks a schedule/dependencies
            and appends relevant metadata to `not_running_dags`.

        Attributes:
            not_running_dags (list): Updated with entries for DAGs that cannot run due
                to either being paused or having no defined schedule/dependencies.

        Workflow:
            1. Finds DAG records in `event_driven_dags` where `ind_root` is `None`.
            2. For each DAG, determines the reason it won't run
                and stores a description of the reason.
            3. Appends a dictionary with the `dag_id`,
                `description`, and `path` to `not_running_dags`.
        """
        stopped_leafs = [
            record for record in self.event_driven_dags if record["ind_root"] is None
        ]
        for leaf in stopped_leafs:
            if leaf["dep_is_paused"]:
                description = "The DAG is paused"
            else:
                description = "The DAG doesn't have a schedule or other dependencies"
            ind_selected_dags = "not_selected_dags"
            if leaf["dep_id"] in self.selected_dags:
                ind_selected_dags = "selected_dags"
            if leaf["dep_id"] not in [dag["dag_id"] for dag in self.next_runs]:
                self.next_runs.append(
                    {
                        "dag_id": leaf["dep_id"],
                        "description": description,
                        "path": leaf["dep_id"],
                        "owner": leaf["deps_owners"],
                        "ind_selected_dags": ind_selected_dags,
                    }
                )

    def update_event_driven_runs_metadata(self, start_dt: datetime, end_dt: datetime):
        """Updates metadata for future runs of event-driven DAGs within a specified
            time range.

        This method handles the prediction and categorization of event-driven DAGs by:
        - Initializing `future_runs` and `not_running_dags` lists to
            store future run metadata
        and DAGs with incomplete dependencies, respectively.
        - Updating metadata for event-driven DAGs.
        - Calculating future runs for DAGs based on dependencies.
        - Adding any missing runs for independent DAG nodes to ensure completeness.

        Args:
            start_dt (datetime): The start of the time range to analyze future runs.
            end_dt (datetime): The end of the time range to analyze future runs.

        Attributes:
            future_runs (list): Contains dictionaries with predicted run
                metadata for event-driven DAGs.
            not_running_dags (list): Stores metadata of DAGs with
                incomplete dependencies, sorted by DAG ID.

        Workflow:
            1. `update_event_driven_dags` is called to refresh
                event-driven DAGs metadata.
            2. `get_future_dependencies_runs` generates future runs
                based on DAG dependencies.
            3. `append_missing_future_independent_nodes_runs` fills in
                any missing runs for independent DAG nodes.
            4. `not_running_dags` is sorted by `dag_id` for easy reference.

        """
        self.future_runs = []
        self.update_event_driven_dags()
        self.get_future_dependencies_runs(start_dt, end_dt)
        self.append_missing_future_independent_nodes_runs()
        self.next_runs = sorted(self.next_runs, key=lambda x: x["dag_id"])

    def get_scheduled_dags_meta_query(self):
        """Constructs a query to fetch metadata and
            historical run data for scheduled DAGs.

        This method builds a SQL query to retrieve metadata for active and unpaused DAGs
        that have a defined schedule interval (excluding dataset-driven DAGs).
        It retrieves information such as the DAG's ID,
            owner, schedule interval, and upcoming run times.
        Additionally, it calculates duration statistics based on the past 30 DAG runs.

        Returns:
            sqlalchemy.sql.selectable.Select:
                A SQLAlchemy query object to fetch scheduled DAGs' metadata,
                including calculated averages and percentiles for run durations.

        Query Fields:
            - `dag_id` (str): Unique identifier for the DAG.
            - `next_dagrun_data_interval_end` (datetime): The end of the data interval
                for the next scheduled DAG run.
            - `owners` (str): Owner of the DAG.
            - `timetable_description` (str):
                Human-readable description of the DAG's timetable.
            - `next_dagrun` (datetime):
                Predicted next run time, combining several fields.
            - `schedule_interval` (str):
                Schedule interval in cron format, if applicable.
            - `avg_duration` (interval): Average duration of recent successful DAG runs.
            - `duration` (interval): Median duration of recent successful DAG runs.

        Notes:
            The query only includes DAGs that are:
            - Marked as active (not paused).
            - Scheduled (not dataset-driven).
            - Limited to a history of the last 30 successful runs,
                using them to calculate average and median durations to
                predict upcoming run times and durations.

        """
        base_query = (
            select(
                DagRun.dag_id,
                DagRun.start_date,
                DagRun.end_date,
                func.row_number()
                .over(partition_by=DagRun.dag_id, order_by=DagRun.start_date.desc())
                .label("row_num"),
            )
            .where(and_(DagRun.state == "success"))
            .alias("base")
        )
        query = (
            select(
                DagModel.dag_id,
                DagModel.next_dagrun_data_interval_end,
                DagModel.owners,
                DagModel.timetable_description,
                DagModel.is_paused,
                coalesce(
                    coalesce(
                        DagModel.next_dagrun, DagModel.next_dagrun_data_interval_start
                    ),
                    DagModel.next_dagrun_data_interval_end,
                ).label("next_dagrun"),
                func.replace(DagModel.schedule_interval, '"', "").label(
                    "schedule_interval"
                ),
                coalesce(
                    func.avg(base_query.c.end_date - base_query.c.start_date),
                    text("INTERVAL '5 minutes'"),
                ).label("avg_duration"),
                coalesce(
                    func.percentile_cont(0.5).within_group(
                        base_query.c.end_date - base_query.c.start_date
                    ),
                    text("INTERVAL '5 minutes'"),
                ).label("duration"),
            )
            .join(
                base_query,
                and_(
                    DagModel.dag_id == base_query.c.dag_id, base_query.c.row_num <= 30
                ),
                isouter=True,
            )
            .where(
                and_(
                    # DagModel.schedule_interval.cast(String) != "null",
                    # DagModel.schedule_interval.cast(String) != '"Dataset"',
                    DagModel.is_active.is_(True),
                    # DagModel.is_paused.is_(False),
                )
            )
            .group_by(
                DagModel.dag_id,
                DagModel.next_dagrun_data_interval_end,
                DagModel.owners,
                DagModel.timetable_description,
                DagModel.schedule_interval,
                DagModel.is_paused,
            )
        )
        return query

    def add_scheduled_run_to_next_runs(
        self, dag, state, timetable_description, ind_selected_dags, cron
    ):
        cron = croniter(cron, datetime.now(timezone.utc))
        run = cron.get_next(datetime)
        dag_info = {
            "dag_id": dag.dag_id,
            "start_time": run if run else None,
            "end_time": (run + dag.duration) if run else None,
            "state": state,
            "owner": dag.owners,  # Fetch owner from conf or use 'unknown'
            "schedule_interval": timetable_description,
            "run_type": "scheduled",
            "duration": str(dag.duration).split(".")[0],
            "ind_selected_dags": ind_selected_dags,
        }
        self.update_next_runs_dict(dag_info)

    def get_scheduled_dags_meta(self, start_dt, end_dt):
        """Fetches metadata for scheduled DAGs and
            predicts future runs within a date range.
        This method compiles and executes a SQL query
            to retrieve metadata for all scheduled DAGs,
            filters them within the specified date range, and calculates future run
            predictions based on the DAGs' schedule intervals.

        Args:
            start_dt (datetime): The start of the date range for DAG run predictions.
            end_dt (datetime): The end of the date range for DAG run predictions.

        Returns:
            list: A list of dictionaries,
                each representing a predicted future DAG run with
                the following structure:
                - `dag_id` (str): Unique identifier for the DAG.
                - `start_time` (datetime): Predicted start time of the DAG run.
                - `end_time` (datetime): Predicted end time,
                    based on `start_time` and `duration`.
                - `state` (str): Set to "forecast" to indicate a predicted run.
                - `owner` (str): Owner of the DAG.
                - `schedule_interval` (str): The cron interval of the DAG.
                - `run_type` (str): Typically "scheduled"
                    for runs created from a schedule.
                - `duration` (str): Estimated run duration based on previous runs.

        Notes:
            The method uses the cron schedule to forecast runs,
            which means it can only predict
            DAG runs with a valid cron expression.

        """
        scheduled_dags_meta_query = self.get_scheduled_dags_meta_query()
        compiled_query = scheduled_dags_meta_query.compile(
            compile_kwargs={"literal_binds": True}
        )
        scheduled_dags = session.execute(str(compiled_query)).all()
        dags_data = []
        for dag in scheduled_dags:
            schedule_interval = dag.schedule_interval
            is_paused = dag.is_paused
            next_run = dag.next_dagrun
            timetable_description = dag.timetable_description
            state = "forecast"
            ind_selected_dags = "not_selected_dags"
            if dag.dag_id in self.selected_dags:
                ind_selected_dags = "selected_dags"
            for new_schedule in self.new_schedules:
                if new_schedule["dag_name"] == dag.dag_id:
                    schedule_interval = new_schedule["cron_schedule"]
                    is_paused = False
                    next_run = datetime.now(timezone.utc)
                    timetable_description = f"Simulator: {schedule_interval}"
                    state = "schedule_simulator"
            cron = self.get_valid_cron(schedule_interval)
            if cron and not is_paused:
                self.add_scheduled_run_to_next_runs(
                    dag, state, timetable_description, ind_selected_dags, cron
                )
                future_runs = self.predict_future_cron_runs(
                    cron, start_dt, end_dt, next_run
                )
                for run in future_runs:
                    dag_info = {
                        "dag_id": dag.dag_id,
                        "start_time": run if run else None,
                        "end_time": (run + dag.duration) if run else None,
                        "state": state,
                        "owner": dag.owners,  # Fetch owner from conf or use 'unknown'
                        "schedule_interval": timetable_description,
                        "run_type": "scheduled",
                        "duration": str(dag.duration).split(".")[0],
                        "ind_selected_dags": ind_selected_dags,
                    }
                    dags_data.append(dag_info)
        return dags_data

    def format_datetime_columns_future_runs(self):
        for future_run in self.next_runs:
            if future_run.get("start_time") and future_run.get("end_time"):
                future_run["start_time"] = future_run["start_time"].isoformat()
                future_run["end_time"] = future_run["end_time"].isoformat()
        for future_run in self.future_runs:
            future_run["start_time"] = future_run["start_time"].isoformat()
            future_run["end_time"] = future_run["end_time"].isoformat()

    def update_predicted_runs(self, start_dt: datetime, end_dt: datetime) -> None:
        """Updates predictions for future DAG runs within a given date range.

        This method retrieves and aggregates metadata on both scheduled and event-driven
        DAG runs, filtering them within the specified time window.
        The resulting data is sorted by start time to provide
        a chronological sequence of forecasted DAG runs.

        Args:
            start_dt (datetime): The start of the date range for predictions.
            end_dt (datetime): The end of the date range for predictions.

        Side Effects:
            Updates `self.future_runs` with a sorted list of dictionaries,
            each representing a DAG run with the following keys:
                - `dag_id` (str): The unique DAG identifier.
                - `start_time` (datetime): The predicted start time.
                - `end_time` (datetime): The predicted end time.
                - `state` (str): Typically set as "forecast" for predicted runs.
                - `owner` (str): The DAG owner.
                - `schedule_interval` (str): The DAG's scheduled interval.
                - `run_type` (str): The run type, typically "scheduled".
                - `duration` (str): The estimated duration for the DAG run.
        """
        self.future_runs = []
        self.next_runs = []
        dags_data = self.get_scheduled_dags_meta(start_dt, end_dt)
        self.update_event_driven_runs_metadata(start_dt, end_dt)
        self.future_runs = dags_data + self.future_runs
        self.format_datetime_columns_future_runs()
        self.future_runs = sorted(self.future_runs, key=lambda x: x["start_time"])

    def get_past_dag_runs(self, start_dt, end_dt):
        """Fetches past DAG run data between specified start and end dates.

        This method queries the database for all DAG runs with
            end dates on or after `start_dt` and start dates on or before `end_dt`.
        It retrieves additional DAG metadata, including
        the schedule interval, owner, and run type.
        Each DAG run's duration is calculated as the
            difference between its start and end times.

        Args:
            start_dt (datetime): The start datetime for filtering past DAG runs.
            end_dt (datetime): The end datetime for filtering past DAG runs.

        Returns:
            list: A list of dictionaries,
                each representing a DAG run with details such as:
                - `dag_id` (str): The ID of the DAG.
                - `start_time` (str): The ISO-formatted start time of the run.
                - `end_time` (str): The ISO-formatted end time of the run.
                - `state` (str): The state of the DAG run.
                - `owner` (str): The owner of the DAG.
                - `schedule_interval` (str): The schedule interval
                    or timetable description.
                - `run_type` (str): The type of the DAG run (e.g., manual, scheduled).
                - `duration` (str): The duration of the DAG run in hours,
                    minutes, and seconds.
        """
        dag_runs = (
            session.query(DagRun)
            .filter(
                and_(
                    coalesce(DagRun.end_date, func.now()) >= start_dt,
                    DagRun.start_date <= end_dt,
                )
            )
            .all()
        )
        dags_data = []
        for run in dag_runs:
            # Fetch the DAG model to get the schedule_interval
            dag_model = (
                session.query(DagModel).filter(DagModel.dag_id == run.dag_id).first()
            )
            ind_selected_dags = "not_selected_dags"
            if run.dag_id in self.selected_dags:
                ind_selected_dags = "selected_dags"
            # Create the dictionary for this DAG run
            dag_info = {
                "dag_id": run.dag_id,
                "start_time": run.start_date.isoformat() if run.start_date else None,
                "end_time": run.end_date.isoformat()
                if run.end_date
                else datetime.now(timezone.utc).isoformat(),
                "state": run.state,
                "owner": dag_model.owners,
                "schedule_interval": dag_model.timetable_description
                if dag_model
                else datetime.now(timezone.utc),
                "run_type": run.run_type,
                "ind_selected_dags": ind_selected_dags,
            }
            dag_info["duration"] = str(
                datetime.fromisoformat(dag_info["end_time"])
                - datetime.fromisoformat(dag_info["start_time"])
            ).split(".")[0]
            dags_data.append(dag_info)
        return dags_data

    def get_dag_runs_data(
        self,
        start_dt: datetime,
        end_dt: datetime,
        show_future_runs: str,
    ) -> list:
        """Fetches data for past and optionally future DAG runs
            within a specified date range.

        Retrieves past DAG runs between the start and end datetimes.
        If `show_future_runs` is set to "true",
            it also includes predicted future DAG runs
            by updating the future runs attribute.

        Args:
            start_dt (datetime): The starting datetime for filtering DAG runs.
            end_dt (datetime): The ending datetime for filtering DAG runs.
            show_future_runs (str): Flag to include future DAG runs if set to "true".

        Returns:
            list: A list of dictionaries representing past and future DAG run data.
        """
        dags_data = self.get_past_dag_runs(start_dt, end_dt)
        if show_future_runs == "true":
            self.update_predicted_runs(start_dt, end_dt)
            dags_data = dags_data + self.future_runs
        dags_data = sorted(dags_data, key=lambda x: x["dag_id"])
        return dags_data

    def get_filter_values(
        self,
        start_dt: datetime,
        end_dt: datetime,
        start: str,
        end_of_time_dt: datetime,
        show_future_runs: str,
    ) -> dict:
        """Generates a filter dictionary based on the provided parameters.

        Constructs a dictionary with formatted start, end, maximum end,
        and future run visibility indicators,
        to be used as filters for Schedule Insidess.

        Args:
            start_dt (datetime): The starting datetime, formatted with minute precision.
            end_dt (datetime): The ending datetime, formatted with minute precision.
            start (str): The start date string; if empty,
                `time_filter["start"]` will be an empty string.
            end_of_time_dt (datetime): The maximum allowable end datetime,
                formatted with minute precision.
            show_future_runs (str): A flag indicating whether to include future runs.

        Returns:
            dict: A dictionary with the following keys:
                - "start" (str): ISO-formatted start datetime,
                    or an empty string if `start` is not provided.
                - "end" (str): ISO-formatted end datetime.
                - "max_end" (str): ISO-formatted maximum end datetime.
                - "show_future_runs" (str): Value of `show_future_runs`.
        """
        filter_values = {}
        filter_values["start"] = (
            start_dt.replace(second=0, microsecond=0).isoformat()
            if start is not None and start != ""
            else ""
        )
        filter_values["end"] = end_dt.replace(second=0, microsecond=0).isoformat()
        filter_values["max_end"] = end_of_time_dt.replace(
            second=0, microsecond=0
        ).isoformat()
        filter_values["show_future_runs"] = show_future_runs
        return filter_values

    def get_params_from_request(self) -> tuple:
        """Extracts relevant parameters from the request arguments.

        Retrieves the following parameters from `request.args`:

        Returns:
            tuple: A tuple containing:
                - start (str or None): The start date,
                    retrieved from `request.args["start"]`.
                - end (str or None): The end date, retrieved from `request.args["end"]`.
                - client_timezone (str or None): The client's timezone,
                    retrieved from `request.args["timezone"]`.
                - show_future_runs (str or None): Boolean-like string indicating
                    whether to show future runs,
                    retrieved from `request.args["show_future_runs"]`.
        """
        start = request.args.get("start")
        end = request.args.get("end")
        client_timezone = request.args.get("timezone")
        show_future_runs = "true"  # request.args.get("show_future_runs")
        dag_names = request.args.getlist("dagName[]")
        cron_schedules = request.args.getlist("cronSchedule[]")
        self.selected_dags = request.args.getlist("selected_dags_filter[]")
        self.new_schedules = [
            {
                "dag_name": dag,
                "cron_schedule": cron,
                "next_custom_run_date": self.get_next_cron_run(cron),
            }
            for dag, cron in zip(dag_names, cron_schedules)
            if self.is_valid_cron(cron)
        ]
        return (start, end, client_timezone, show_future_runs)

    def get_all_active_dags(self):
        active_dags = session.query(DagModel).filter(DagModel.is_active.is_(True)).all()
        active_dags_names = [run.dag_id for run in active_dags]
        active_dags_names = sorted(active_dags_names)
        return active_dags_names

    @expose("/")
    def main(self):
        """Sets a default start time and redirects to `schedule_insights`.

        This default method calculates a `start` time as 4 hours before the current
        UTC time and redirects to `schedule_insights`, passing this calculated `start`
            time.

        Returns:
            werkzeug.wrappers.Response: Redirect response to `self.schedule_insights()`.
        """
        time_limit = datetime.now(timezone.utc) - timedelta(hours=4)
        return redirect(
            url_for(
                "ScheduleInsightsAppBuilderBaseView.schedule_insights",
                start=time_limit.isoformat(),
            )
        )

    @expose("/schedule_insights")
    def schedule_insights(self):
        """Visualizes past and future DAG runs

        Args:
            start (str): The start date, retrieved from `request.args["start"]`.
            end (str): The end date, retrieved from `request.args["end"]`.
            timezone (str): Client's local timezone,
                retrieved from `request.args["timezone"]`.
            show_future_runs (str): Boolean-like string
                that defines whether to calculate future runs,
                retrieved from `request.args["show_future_runs"]`.

        Returns:
            Rendered HTML page with two tables (future runs, missing future runs)
            and gantt chart
        """
        (
            start,
            end,
            client_timezone,
            show_future_runs,
        ) = self.get_params_from_request()
        start_dt, end_dt, end_of_time_dt = self.get_filter_dates(
            start, end, client_timezone
        )
        filter_values = self.get_filter_values(
            start_dt, end_dt, start, end_of_time_dt, show_future_runs
        )
        all_active_dags = self.get_all_active_dags()
        dags_data = self.get_dag_runs_data(start_dt, end_dt, show_future_runs)
        return self.render_template(
            "schedule_insights.html",
            dags_data=dags_data,
            filter_values=filter_values,
            next_runs=self.next_runs,
            future_runs=self.future_runs,
            all_active_dags=all_active_dags,
            new_schedules=self.new_schedules,
            selected_dags=self.selected_dags,
        )

    @expose("/get_future_runs_json")
    def get_future_runs_json(self):
        start, end, client_timezone, show_future_runs = self.get_params_from_request()
        timedelta_filter_hours = int(request.args.get("timedelta_filter_hours", "24"))
        dag_id = request.args.get("dag_id")
        start = datetime.now(timezone.utc).isoformat()
        end = (
            datetime.now(timezone.utc) + timedelta(hours=timedelta_filter_hours)
        ).isoformat()
        start_dt, end_dt, end_of_time_dt = self.get_filter_dates(
            start, end, client_timezone
        )
        self.update_predicted_runs(start_dt, end_dt)
        if dag_id:
            self.future_runs = [
                run for run in self.future_runs if run["dag_id"] == dag_id
            ]
        return jsonify(self.future_runs)

    @expose("/get_next_future_run_json")
    def get_next_future_run_json(self):
        start, end, client_timezone, show_future_runs = self.get_params_from_request()
        dag_id = request.args.get("dag_id")
        start = datetime.now(timezone.utc).isoformat()
        start_dt, end_dt, end_of_time_dt = self.get_filter_dates(
            start, end, client_timezone
        )
        self.update_predicted_runs(start_dt, end_dt)
        if dag_id:
            self.next_runs = [run for run in self.next_runs if run["dag_id"] == dag_id]
        return jsonify(self.next_runs)


v_appbuilder_view = ScheduleInsightsAppBuilderBaseView()
v_appbuilder_package = {
    "name": "Schedule Insides",
    "category": "Browse",
    "view": v_appbuilder_view,
}


class ScheduleInsightsPlugin(AirflowPlugin):
    name = "schedule_insights"
    hooks = []
    macros = []
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
    appbuilder_menu_items = []
