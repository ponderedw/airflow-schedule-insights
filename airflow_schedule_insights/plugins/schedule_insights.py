from typing import List

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.session import create_session
from airflow.models import DagRun, DagModel
from sqlalchemy import and_, func, select, text
from croniter import croniter, CroniterBadCronError
from datetime import datetime, timedelta, timezone
import os

app = FastAPI()

template_dir = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=template_dir)


# ---------------------------------------------------------------------------
# Schedule helpers
# ---------------------------------------------------------------------------

class ScheduleManager:
    @staticmethod
    def is_valid_cron(s):
        try:
            croniter(s)
            return True
        except (CroniterBadCronError, TypeError, ValueError):
            return False

    @staticmethod
    def get_valid_cron(timetable_summary):
        """Return the first valid cron expression found in timetable_summary.

        "*/30 * * * *"          → "*/30 * * * *"
        "Asset or */40 * * * *" → "*/40 * * * *"
        "Asset"                 → None
        "None"                  → None
        """
        if not timetable_summary or timetable_summary in ("None", "Asset"):
            return None
        for part in str(timetable_summary).split(" or "):
            part = part.strip()
            if ScheduleManager.is_valid_cron(part):
                return part
        return None

    @staticmethod
    def predict_future_cron_runs(cron_schedule, start_dt, end_dt, anchor=None, max_runs=1000):
        if anchor is None:
            anchor = datetime.now(timezone.utc)
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=timezone.utc)
        cron = croniter(cron_schedule, anchor)
        effective_start = max(datetime.now(timezone.utc), start_dt)
        runs = []
        count = 0
        while count < max_runs:
            nxt = cron.get_next(datetime)
            if nxt > end_dt:
                break
            if nxt >= effective_start:
                runs.append(nxt)
            count += 1
        return runs


# ---------------------------------------------------------------------------
# Asset-expression evaluator
# ---------------------------------------------------------------------------

def _ensure_utc(dt):
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def format_asset_expr(expr):
    """Return human-readable string for asset_expression.

    {"asset": {"uri": "dag_a"}}                   → "dag_a"
    {"all": [{"asset":...}, {"asset":...}]}        → "(dag_a AND dag_b)"
    {"any": [{"asset":...}, {"asset":...}]}        → "(dag_a OR dag_b)"
    """
    if not expr:
        return ""
    if "asset" in expr:
        return expr["asset"].get("uri") or expr["asset"].get("name") or "?"
    if "all" in expr:
        parts = [format_asset_expr(sub) for sub in expr["all"]]
        return "(" + " AND ".join(parts) + ")" if len(parts) > 1 else (parts[0] if parts else "")
    if "any" in expr:
        parts = [format_asset_expr(sub) for sub in expr["any"]]
        return "(" + " OR ".join(parts) + ")" if len(parts) > 1 else (parts[0] if parts else "")
    return ""


def find_paused_upstream(expr, asset_uri_to_producer, dag_meta):
    """Return sorted list of paused producer dag_ids anywhere in the dependency chain."""
    if not expr:
        return []
    if "asset" in expr:
        uri = expr["asset"].get("uri") or expr["asset"].get("name")
        producer = asset_uri_to_producer.get(uri)
        if producer and dag_meta.get(producer, {}).get("is_paused"):
            return [producer]
        return []
    result = set()
    for sub in expr.get("all", []) + expr.get("any", []):
        result.update(find_paused_upstream(sub, asset_uri_to_producer, dag_meta))
    return sorted(result)


def evaluate_asset_expr(expr, asset_to_producer_finish, visited=None):
    """Recursively evaluate an asset_expression JSON object.

    Returns the earliest datetime when the condition is satisfied, or None.

    expr examples:
      {"all": [{"asset": {"uri": "..."}}, ...]}
      {"any": [{"asset": {"uri": "..."}}, ...]}
      {"asset": {"uri": "..."}}
    """
    if expr is None:
        return None
    if visited is None:
        visited = set()

    if "asset" in expr:
        uri = expr["asset"].get("uri") or expr["asset"].get("name")
        return _ensure_utc(asset_to_producer_finish.get(uri))

    if "all" in expr:
        times = [evaluate_asset_expr(sub, asset_to_producer_finish, visited) for sub in expr["all"]]
        valid = [t for t in times if t is not None]
        return max(valid) if valid else None

    if "any" in expr:
        times = [evaluate_asset_expr(sub, asset_to_producer_finish, visited) for sub in expr["any"]]
        valid = [t for t in times if t is not None]
        return min(valid) if valid else None

    return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def main_view(request: Request, start: str = None):
    if not start:
        start = (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat()

    with create_session() as session:
        active_dags = list(
            session.execute(
                select(DagModel.dag_id)
                .where(DagModel.is_stale.is_(False))
                .order_by(DagModel.dag_id)
            ).scalars().all()
        )

    return templates.TemplateResponse("schedule_insights.html", {
        "request": request,
        "all_active_dags": active_dags,
        "filter_values": {"start": start},
    })


@app.get("/api/predicted_runs")
async def get_predicted_runs(
    start: str = Query(None),
    end: str = Query(None),
    timezone_str: str = Query("UTC"),
    selected_dags: List[str] = Query(default=[]),
    dag_name: List[str] = Query(default=[]),
    cron_schedule: List[str] = Query(default=[]),
):
    now = datetime.now(timezone.utc)
    start_dt = datetime.fromisoformat(start) if start else now - timedelta(hours=4)
    end_dt = datetime.fromisoformat(end) if end else now + timedelta(hours=24)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    # Build schedule simulator overrides: {dag_id: cron_expression}
    simulator_overrides = {}
    for d, c in zip(dag_name, cron_schedule):
        if d and c and ScheduleManager.is_valid_cron(c):
            simulator_overrides[d] = c

    selected_set = set(selected_dags)

    with create_session() as session:
        # ------------------------------------------------------------------ #
        # 1a. DAG metadata
        # ------------------------------------------------------------------ #
        dag_info_rows = list(session.execute(
            select(
                DagModel.dag_id,
                DagModel.owners,
                DagModel.timetable_summary,
                DagModel.timetable_description,
                DagModel.is_paused,
                DagModel.asset_expression,
                DagModel.next_dagrun,
                DagModel.next_dagrun_create_after,
                DagModel.next_dagrun_data_interval_end,
            )
            .where(DagModel.is_stale.is_(False))
        ).all())

        # ------------------------------------------------------------------ #
        # 1b. Median successful-run duration per DAG
        # ------------------------------------------------------------------ #
        duration_rows = list(session.execute(
            select(
                DagRun.dag_id,
                func.percentile_cont(0.5)
                .within_group(DagRun.end_date - DagRun.start_date)
                .label("median_duration"),
            )
            .where(DagRun.state == "success")
            .group_by(DagRun.dag_id)
        ).all())
        duration_by_dag = {row.dag_id: row.median_duration for row in duration_rows}

        # ------------------------------------------------------------------ #
        # 2. Asset → producing DAG mapping (task_outlet_asset_reference)
        # ------------------------------------------------------------------ #
        producer_rows = session.execute(
            text("""
                SELECT a.uri AS asset_uri, toar.dag_id AS producer_dag_id
                FROM task_outlet_asset_reference toar
                JOIN asset a ON toar.asset_id = a.id
            """)
        ).all()
        asset_uri_to_producer = {row.asset_uri: row.producer_dag_id for row in producer_rows}

        # ------------------------------------------------------------------ #
        # 3. Past DAG runs for the Gantt chart
        # ------------------------------------------------------------------ #
        past_run_rows = list(session.execute(
            select(
                DagRun.dag_id,
                DagRun.start_date,
                DagRun.end_date,
                DagRun.state,
                DagRun.run_type,
            )
            .where(
                and_(
                    func.coalesce(DagRun.end_date, func.now()) >= start_dt,
                    DagRun.start_date <= end_dt,
                )
            )
            .order_by(DagRun.start_date)
        ).all())

    # ------------------------------------------------------------------ #
    # 4. Build per-DAG metadata dict
    # ------------------------------------------------------------------ #
    dag_meta = {}
    for row in dag_info_rows:
        anchor = (
            row.next_dagrun
            or row.next_dagrun_create_after
            or row.next_dagrun_data_interval_end
        )
        median_dur = duration_by_dag.get(row.dag_id) or timedelta(minutes=5)

        # Apply simulator override: treat as unpaused cron DAG with custom schedule
        sim_cron = simulator_overrides.get(row.dag_id)
        effective_timetable = sim_cron if sim_cron else row.timetable_summary
        effective_paused = False if sim_cron else row.is_paused

        dag_meta[row.dag_id] = {
            "dag_id": row.dag_id,
            "owners": row.owners,
            "timetable_summary": effective_timetable,
            "timetable_description": (
                f"Simulator: {sim_cron}" if sim_cron else row.timetable_description
            ),
            "is_paused": effective_paused,
            "is_simulator": sim_cron is not None,
            "asset_expression": row.asset_expression,
            "anchor": _ensure_utc(anchor) if not sim_cron else now,
            "median_duration": median_dur,
        }

    # ------------------------------------------------------------------ #
    # 5. Compute each DAG's estimated next-finish time for asset propagation
    #    NOTE: we compute finish times for ALL DAGs (including paused ones)
    #    so that multi-level asset chains can be resolved. Paused DAGs are
    #    excluded from forecast bars further below.
    # ------------------------------------------------------------------ #
    dag_finish = {}  # dag_id → datetime

    # Pass 1: cron DAGs (including simulators and paused ones)
    for dag_id, meta in dag_meta.items():
        cron_str = ScheduleManager.get_valid_cron(meta["timetable_summary"])
        if not cron_str:
            continue
        anchor = meta["anchor"] or now
        runs = ScheduleManager.predict_future_cron_runs(cron_str, now, end_dt, anchor, max_runs=1)
        if runs:
            dag_finish[dag_id] = runs[0] + meta["median_duration"]

    # Passes 2-N: propagate through asset-triggered DAGs (paused included)
    for _ in range(10):
        asset_to_finish = {
            asset_uri: dag_finish[producer]
            for asset_uri, producer in asset_uri_to_producer.items()
            if producer in dag_finish
        }
        changed = False
        for dag_id, meta in dag_meta.items():
            if dag_id in dag_finish:
                continue
            expr = meta["asset_expression"]
            if not expr:
                continue
            trigger_time = evaluate_asset_expr(expr, asset_to_finish)
            if trigger_time is not None:
                dag_finish[dag_id] = trigger_time + meta["median_duration"]
                changed = True
        if not changed:
            break

    # Rebuild asset_to_finish for forecast entries
    asset_to_finish = {
        asset_uri: dag_finish[producer]
        for asset_uri, producer in asset_uri_to_producer.items()
        if producer in dag_finish
    }

    # ------------------------------------------------------------------ #
    # 6. Collect all runs
    # ------------------------------------------------------------------ #
    all_runs = []

    def ind_selected(dag_id):
        return "selected_dags" if (selected_set and dag_id in selected_set) else "not_selected_dags"

    # --- Past actual runs ---
    for run in past_run_rows:
        start_time = _ensure_utc(run.start_date)
        end_time = _ensure_utc(run.end_date) or now
        meta = dag_meta.get(run.dag_id, {})
        all_runs.append({
            "dag_id": run.dag_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "state": run.state,
            "owner": meta.get("owners", ""),
            "run_type": run.run_type,
            "duration": str(end_time - start_time).split(".")[0],
            "ind_selected_dags": ind_selected(run.dag_id),
        })

    # --- Future cron-scheduled runs ---
    for dag_id, meta in dag_meta.items():
        if meta["is_paused"]:
            continue
        cron_str = ScheduleManager.get_valid_cron(meta["timetable_summary"])
        if not cron_str:
            continue
        runs = ScheduleManager.predict_future_cron_runs(cron_str, start_dt, end_dt, meta["anchor"])
        duration = meta["median_duration"]
        state = "schedule_simulator" if meta["is_simulator"] else "forecast"
        for r in runs:
            all_runs.append({
                "dag_id": dag_id,
                "start_time": r.isoformat(),
                "end_time": (r + duration).isoformat(),
                "state": state,
                "owner": meta["owners"],
                "run_type": "scheduled",
                "schedule_interval": meta["timetable_description"],
                "duration": str(duration).split(".")[0],
                "ind_selected_dags": ind_selected(dag_id),
            })

    # --- Future asset-triggered runs ---
    for dag_id, meta in dag_meta.items():
        if meta["is_paused"]:
            continue
        if ScheduleManager.get_valid_cron(meta["timetable_summary"]):
            continue  # already covered as cron
        expr = meta["asset_expression"]
        if not expr:
            continue
        trigger_time = evaluate_asset_expr(expr, asset_to_finish)
        if trigger_time is None or trigger_time > end_dt:
            continue
        if trigger_time < start_dt:
            trigger_time = start_dt
        duration = meta["median_duration"]
        all_runs.append({
            "dag_id": dag_id,
            "start_time": trigger_time.isoformat(),
            "end_time": (trigger_time + duration).isoformat(),
            "state": "forecast",
            "owner": meta["owners"],
            "run_type": "asset_triggered",
            "schedule_interval": format_asset_expr(expr),
            "duration": str(duration).split(".")[0],
            "ind_selected_dags": ind_selected(dag_id),
        })

    # Sort all runs by start_time (chronological order across all DAGs)
    all_runs.sort(key=lambda x: x["start_time"])

    # ------------------------------------------------------------------ #
    # 7. Compute next_runs — one entry per active DAG
    # ------------------------------------------------------------------ #
    next_runs = []
    for dag_id, meta in sorted(dag_meta.items()):
        entry = {
            "dag_id": dag_id,
            "owner": meta["owners"],
            "schedule_interval": meta["timetable_description"],
            "start_time": None,
            "end_time": None,
            "state": None,
            "run_type": None,
            "duration": None,
            "description": None,
            "ind_selected_dags": ind_selected(dag_id),
        }

        if meta["is_paused"] and not meta["is_simulator"]:
            entry["description"] = "The DAG is paused"
            next_runs.append(entry)
            continue

        cron_str = ScheduleManager.get_valid_cron(meta["timetable_summary"])
        if cron_str:
            anchor = meta["anchor"] or now
            upcoming = ScheduleManager.predict_future_cron_runs(cron_str, now, end_dt, anchor, max_runs=1)
            if upcoming:
                r = upcoming[0]
                duration = meta["median_duration"]
                entry.update({
                    "start_time": r.isoformat(),
                    "end_time": (r + duration).isoformat(),
                    "state": "schedule_simulator" if meta["is_simulator"] else "forecast",
                    "run_type": "scheduled",
                    "duration": str(duration).split(".")[0],
                })
        elif meta["asset_expression"]:
            expr = meta["asset_expression"]
            trigger_label = format_asset_expr(expr)
            paused_up = find_paused_upstream(expr, asset_uri_to_producer, dag_meta)
            trigger_time = evaluate_asset_expr(expr, asset_to_finish)
            if trigger_time is not None:
                duration = meta["median_duration"]
                entry.update({
                    "start_time": trigger_time.isoformat(),
                    "end_time": (trigger_time + duration).isoformat(),
                    "state": "forecast",
                    "run_type": "asset_triggered",
                    "duration": str(duration).split(".")[0],
                    "trigger_assets": trigger_label,
                    "paused_upstream": paused_up if paused_up else None,
                })
            else:
                entry["description"] = f"Cannot predict: {trigger_label}"
                entry["trigger_assets"] = trigger_label
                entry["paused_upstream"] = paused_up if paused_up else None
        else:
            entry["description"] = "No schedule or dependencies"

        next_runs.append(entry)

    return JSONResponse(content={"runs": all_runs, "next_runs": next_runs})


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

class ScheduleInsightsPlugin(AirflowPlugin):
    name = "schedule_insights_plugin"
    fastapi_apps = [{
        "app": app,
        "url_prefix": "/schedule_insights",
        "name": "Schedule Insights",
    }]
    external_views = [{
        "name": "Schedule Insights",
        "href": "/schedule_insights/",
        "destination": "nav",
        "category": "browse",
        "url_route": "schedule_insights",
    }]
