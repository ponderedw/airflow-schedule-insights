# Airflow Schedule Insides Plugin

The Airflow Schedule Insides Plugin for [Apache Airflow](https://github.com/apache/airflow) allows you to visualize DAG runs in a Gantt chart, predict future runs, and identify DAGs that won't run, providing a seamless and efficient workflow for managing your pipelines. Enhance your workflow monitoring and planning with intuitive visualizations.

[![Tests Status](https://github.com/hipposys-ltd/airflow-schedule-insights/workflows/Tests/badge.svg)](https://github.com/hipposys-ltd/airflow-schedule-insights/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## System Requirements

- **Airflow Versions**: 2.4.0 or newer

## How to Install

Add `airflow-schedule-insights` to your `requirements.txt` and restart the web server.

## How to Use

1. Navigate to `Schedule Insides` in the `Browse` tab to access the plugin:

   ![Menu](https://github.com/hipposys-ltd/airflow-schedule-insights/releases/download/v0.1.0-alpha.0/plugin_menu.png)

2. View all DAG runs in a Gantt chart:

   ![Gantt Chart Logs](https://github.com/hipposys-ltd/airflow-schedule-insights/releases/download/v0.1.0-alpha.0/gantt_chart_history_logs.png)

3. Toggle the `Show Future Runs?` option to predict the next runs for your DAGs and generate a list of all the DAGs that won't run.

   **Note**: All event-driven DAGs (scheduled by datasets and triggers) are predicted only to their next run.

4. Future DAGs will be highlighted in gray on the Gantt chart:

   ![Gantt Chart Future Runs](https://github.com/hipposys-ltd/airflow-schedule-insights/releases/download/v0.1.0-alpha.0/gantt_chart_future_runs.png)

5. A table of future runs will be displayed, with events ordered by their start date:

   ![Future Runs Table](https://github.com/hipposys-ltd/airflow-schedule-insights/releases/download/v0.1.0-alpha.0/future_runs_table.png)

6. Below this, you will find a table listing all the DAGs that won't run:

   ![Missing Future Runs Table](https://github.com/hipposys-ltd/airflow-schedule-insights/releases/download/v0.1.0-alpha.0/missing_future_runs_table.png)
