# Academic Data Platform — Airflow DAGs

This directory contains the DAGs that power the academic data pipeline. The pipeline ingests data from source systems (SIS, LMS, exam platforms), normalises and transforms it, and publishes institutional reports for analytics and compliance.

---

## Pipeline Overview

```
load_student_records (*/10)──────────────────────────────┐
                                                          ▼
load_exam_results (*/25)──► normalize_raw_academic_data ──► transform_assessment_scores ──► consolidate_academic_records (paused)
                (triggers)       (*/30)                   │         (& normalize)             (& transform_assessment_scores)
                                                          │                                          │
                                                          └──► transform_student_performance ✗       ├──► transform_enrollment_metrics ──► publish_institutional_reports
                                                                 (& normalize, always fails)         │
                                                                                                     ├──► transform_exam_analytics (*/40 OR asset)
                                                                                                     └──► (feed into publish_institutional_reports)

load_staff_roster (manual)   — standalone, no downstream
load_lms_activity (manual, paused) — standalone, no downstream
```

---

## DAGs

### Ingestion Layer

#### `load_student_records`
- **File:** `load_student_records.py`
- **Schedule:** `*/10 * * * *` (every 10 minutes)
- **Owner:** Data Engineering
- **Description:** Pulls the latest student enrolment and demographic records from the Student Information System (SIS). Emits the `load_student_records` dataset on each successful run, which can independently trigger downstream assessment transforms when exam data is not yet available.
- **Outlets:** `load_student_records`

---

#### `load_exam_results`
- **File:** `load_exam_results.py`
- **Schedule:** `*/25 * * * *` (every 25 minutes)
- **Owner:** Data Engineering
- **Description:** Fetches raw exam and assessment result files from the exam delivery platform. On completion it emits the `load_exam_results` dataset and also fires a `TriggerDagRunOperator` to kick off `normalize_raw_academic_data` immediately, without waiting for that DAG's own cron tick.
- **Outlets:** `load_exam_results`
- **Triggers:** `normalize_raw_academic_data`

---

#### `load_staff_roster`
- **File:** `load_staff_roster.py`
- **Schedule:** `None` (manual trigger only)
- **Owner:** HR Systems
- **Description:** Loads the current staff and faculty roster from the HR system. Run on demand — typically after a bulk hire, restructure, or start-of-term onboarding. Has no dataset outlets and no downstream dependencies.

---

#### `load_lms_activity`
- **File:** `load_lms_activity.py`
- **Schedule:** `None` (manual trigger only)
- **Status:** **Paused on creation**
- **Owner:** Data Engineering
- **Description:** Ingests student activity events from the Learning Management System (LMS), including logins, assignment submissions, and video engagement. Paused by default; enable when LMS integration is active and validated.

---

### Normalisation Layer

#### `normalize_raw_academic_data`
- **File:** `normalize_raw_academic_data.py`
- **Schedule:** `*/30 * * * *` (every 30 minutes) — also triggered directly by `load_exam_results`
- **Owner:** Data Engineering
- **Description:** Applies schema normalisation, deduplication, and referential integrity checks across all raw ingested academic data. Acts as the central staging anchor that downstream transforms depend on. Both its own cron schedule and an explicit trigger from `load_exam_results` can start it.
- **Outlets:** `normalize_raw_academic_data`

---

### Transformation Layer

#### `transform_assessment_scores`
- **File:** `transform_assessment_scores.py`
- **Schedule:** Dataset — `(normalize_raw_academic_data AND load_exam_results) OR load_student_records`
- **Owner:** Academic Analytics
- **Description:** Calculates standardised assessment scores, applies grade-boundary logic, and attaches student metadata. The OR condition ensures this can proceed even if exam data arrives on a different cadence — falling back to student record updates alone is sufficient to recompute.
- **Outlets:** `transform_assessment_scores`

---

#### `transform_student_performance`
- **File:** `transform_student_performance.py`
- **Schedule:** Dataset — `normalize_raw_academic_data AND transform_assessment_scores`
- **Owner:** Academic Analytics
- **Description:** Builds longitudinal student performance profiles by combining normalised academic data with scored assessments. Computes progression risk flags, attendance correlation, and cohort benchmarks.
- **Outlets:** `transform_student_performance`
- **Note:** **This DAG is currently expected to fail** (division-by-zero in the aggregation step). It is kept active to represent a known broken pipeline stage during development.

---

#### `transform_exam_analytics`
- **File:** `transform_exam_analytics.py`
- **Schedule:** `*/40 * * * *` OR Dataset `consolidate_academic_records` (whichever comes first)
- **Owner:** Exams Office
- **Description:** Produces per-exam and per-cohort analytics: pass rates, score distributions, item-difficulty indices, and examiner workload summaries. The hybrid schedule means it refreshes on its own cadence and also reacts immediately whenever a new consolidated record set is ready.
- **Outlets:** `transform_exam_analytics`

---

#### `transform_enrollment_metrics`
- **File:** `transform_enrollment_metrics.py`
- **Schedule:** Dataset — `consolidate_academic_records`
- **Owner:** Registry Office
- **Description:** Derives enrolment KPIs from the consolidated academic record: headcount by programme and level, retention rates, deferral counts, and predicted completion rates. Feeds directly into the institutional reporting layer.
- **Outlets:** `transform_enrollment_metrics`

---

### Consolidation & Reporting Layer

#### `consolidate_academic_records`
- **File:** `consolidate_academic_records.py`
- **Schedule:** Dataset — `normalize_raw_academic_data AND transform_assessment_scores`
- **Status:** **Paused on creation**
- **Owner:** Data Engineering
- **Description:** Merges normalised raw data with scored assessments to produce the authoritative academic record store. This is the central asset consumed by both the exam analytics and enrolment metrics transforms, as well as the final reporting DAG. Enable once upstream data quality checks pass in a new environment.
- **Outlets:** `consolidate_academic_records`

---

#### `publish_institutional_reports`
- **File:** `publish_institutional_reports.py`
- **Schedule:** Dataset — `consolidate_academic_records AND transform_enrollment_metrics`
- **Owner:** Institutional Research
- **Description:** Final stage of the pipeline. Assembles accreditation reports, governing-body dashboards, and statutory compliance exports (e.g. HESA returns). Only runs after both the authoritative record store and enrolment metrics are confirmed fresh, ensuring reports are never based on partial data.
- **Outlets:** `publish_institutional_reports`

---

## Dependency Map

| DAG | Depends on | Consumed by |
|-----|-----------|-------------|
| `load_student_records` | — | `transform_assessment_scores` |
| `load_exam_results` | — | `normalize_raw_academic_data` (trigger + dataset), `transform_assessment_scores` |
| `load_staff_roster` | — | — |
| `load_lms_activity` | — | — |
| `normalize_raw_academic_data` | cron / `load_exam_results` trigger | `transform_assessment_scores`, `transform_student_performance`, `consolidate_academic_records` |
| `transform_assessment_scores` | `normalize_raw_academic_data` & `load_exam_results` OR `load_student_records` | `transform_student_performance`, `consolidate_academic_records` |
| `transform_student_performance` | `normalize_raw_academic_data` & `transform_assessment_scores` | — |
| `consolidate_academic_records` | `normalize_raw_academic_data` & `transform_assessment_scores` | `transform_exam_analytics`, `transform_enrollment_metrics`, `publish_institutional_reports` |
| `transform_exam_analytics` | cron OR `consolidate_academic_records` | — |
| `transform_enrollment_metrics` | `consolidate_academic_records` | `publish_institutional_reports` |
| `publish_institutional_reports` | `consolidate_academic_records` & `transform_enrollment_metrics` | — |

---

## Schedules at a Glance

| DAG | Schedule |
|-----|----------|
| `load_student_records` | Every 10 min |
| `load_exam_results` | Every 25 min |
| `load_staff_roster` | Manual |
| `load_lms_activity` | Manual (paused) |
| `normalize_raw_academic_data` | Every 30 min |
| `transform_assessment_scores` | Dataset-driven |
| `transform_student_performance` | Dataset-driven |
| `consolidate_academic_records` | Dataset-driven (paused) |
| `transform_exam_analytics` | Every 40 min + dataset |
| `transform_enrollment_metrics` | Dataset-driven |
| `publish_institutional_reports` | Dataset-driven |
