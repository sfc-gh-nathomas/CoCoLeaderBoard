#!/usr/bin/env python3
"""
AMSExpansion Adoption Leaderboard Generator
Queries Snowflake for Cortex Code & Snowflake Intelligence adoption data,
generates a self-contained HTML leaderboard, and opens it in the browser.

Usage:
    pip install snowflake-connector-python
    python generate_leaderboard.py --connection MyConnection

    # Or with explicit warehouse:
    python generate_leaderboard.py --connection MyConnection --warehouse CORPORATE_SE_WH
"""

import argparse
import json
import os
import sys
import webbrowser
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    import snowflake.connector
except ImportError:
    print("ERROR: snowflake-connector-python is required.")
    print("Install it with:  pip install snowflake-connector-python")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Snowflake connection helper
# ---------------------------------------------------------------------------

def get_connection(connection_name: str, warehouse: str | None = None):
    """Connect to Snowflake using ~/.snowflake/connections.toml config."""
    try:
        conn = snowflake.connector.connect(connection_name=connection_name)
    except Exception as e:
        # Fallback: try reading toml manually
        toml_path = Path.home() / ".snowflake" / "connections.toml"
        if not toml_path.exists():
            print(f"ERROR: Could not connect with name '{connection_name}'.")
            print(f"  Ensure ~/.snowflake/connections.toml exists with a [{connection_name}] section.")
            print(f"  Original error: {e}")
            sys.exit(1)

        try:
            import tomllib
        except ImportError:
            import tomli as tomllib  # Python < 3.11

        with open(toml_path, "rb") as f:
            config = tomllib.load(f)

        if connection_name not in config:
            print(f"ERROR: Connection '{connection_name}' not found in {toml_path}")
            print(f"  Available connections: {list(config.keys())}")
            sys.exit(1)

        params = dict(config[connection_name])
        # Map common toml keys to connector params
        key_map = {"accountname": "account", "username": "user"}
        for old, new in key_map.items():
            if old in params and new not in params:
                params[new] = params.pop(old)

        conn = snowflake.connector.connect(**params)

    if warehouse:
        conn.cursor().execute(f"USE WAREHOUSE {warehouse}")
    return conn


def run_query(conn, sql: str) -> list[dict]:
    """Execute SQL and return list of dicts."""
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# SQL Queries
# ---------------------------------------------------------------------------

SE_ATTR_CTE = """
se_attr AS (
  SELECT * FROM SALES.SE_REPORTING.DIM_ACCOUNTS_SE_ATTRIBUTES_DAILY
  WHERE DS = (SELECT MAX(DS) FROM SALES.SE_REPORTING.DIM_ACCOUNTS_SE_ATTRIBUTES_DAILY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY SALES_ENGINEER_NAME NULLS LAST) = 1
)"""

COCO_CTE = """
coco AS (
  SELECT
    SALESFORCE_ACCOUNT_ID,
    SUM(ESTIMATED_CREDITS) as total_credits,
    COUNT(*) as total_requests
  FROM SNOWSCIENCE.LLM.CORTEX_CODE_REQUEST_FACT
  WHERE DS >= DATEADD('day', -90, CURRENT_DATE())
    AND SALESFORCE_ACCOUNT_ID IS NOT NULL
    AND AGREEMENT_TYPE NOT IN ('Trial','Internal')
    AND IS_ACTIVE_CAPACITY_FINANCE = TRUE
  GROUP BY 1
)"""

SI_AGG_CTE = """
si_agg AS (
  SELECT
    SALESFORCE_ACCOUNT_NAME,
    SUM(CREDITS_LAST_90_DAYS) as total_credits,
    SUM(ACTIVE_USERS_LAST_90_DAYS) as total_users
  FROM SALES.DEV.BOB_SNOWFLAKE_INTELLIGENCE_USAGE_STREAMLIT_AGG
  WHERE CREDITS_LAST_90_DAYS > 0
  GROUP BY 1
)"""

SI_DETAIL_CTE = """
si_detail AS (
  SELECT
    SALESFORCE_ACCOUNT_NAME,
    COUNT(DISTINCT REQUEST_ID) as si_questions,
    COUNT(DISTINCT CASE WHEN DS >= DATEADD('day', -7, CURRENT_DATE()) THEN USER_ID END) as wau
  FROM SNOWSCIENCE.LLM.SNOWFLAKE_INTELLIGENCE_ACCOUNTS_CREDITS
  GROUP BY 1
)"""


def q_ae_coco():
    return f"""
WITH {SE_ATTR_CTE},
{COCO_CTE},
base AS (
  SELECT
    r.ACCOUNT_OWNER_NAME as ae,
    se.SALES_ENGINEER_NAME as se_name,
    se.REGION_NAME as region,
    se.DISTRICT_NAME as district,
    se.PATCH_NAME as patch,
    COUNT(DISTINCT CASE WHEN c.SALESFORCE_ACCOUNT_ID IS NOT NULL THEN r.SALESFORCE_ACCOUNT_ID END) as accts_with_coco,
    COUNT(DISTINCT r.SALESFORCE_ACCOUNT_ID) as total_accts,
    SUM(COALESCE(c.total_credits, 0)) as total_credits,
    SUM(COALESCE(c.total_requests, 0)) as total_requests
  FROM SALES.RAVEN.ACCOUNT r
  JOIN se_attr se ON se.ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
  LEFT JOIN coco c ON c.SALESFORCE_ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
  WHERE r.THEATER = 'AMSExpansion'
    AND r.ACCOUNT_OWNER_FUNCTION_C = 'Account Executive'
  GROUP BY 1, 2, 3, 4, 5
)
SELECT ae, se_name as se, region, district, patch,
       accts_with_coco as accts, total_accts as "totAccts",
       ROUND(total_credits, 2) as cr,
       total_requests as reqs
FROM base
WHERE accts_with_coco > 0
ORDER BY cr DESC
LIMIT 5"""


def q_ae_si():
    return f"""
WITH {SE_ATTR_CTE},
{SI_AGG_CTE},
{SI_DETAIL_CTE},
base AS (
  SELECT
    r.ACCOUNT_OWNER_NAME as ae,
    se.SALES_ENGINEER_NAME as se_name,
    se.REGION_NAME as region,
    se.DISTRICT_NAME as district,
    se.PATCH_NAME as patch,
    COUNT(DISTINCT CASE WHEN sa.SALESFORCE_ACCOUNT_NAME IS NOT NULL THEN r.NAME END) as accts_with_si,
    COUNT(DISTINCT r.SALESFORCE_ACCOUNT_ID) as total_accts,
    SUM(COALESCE(sa.total_credits, 0)) as total_credits,
    SUM(COALESCE(sa.total_users, 0)) as total_users,
    SUM(COALESCE(sd.si_questions, 0)) as si_questions,
    SUM(COALESCE(sd.wau, 0)) as wau
  FROM SALES.RAVEN.ACCOUNT r
  JOIN se_attr se ON se.ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
  LEFT JOIN si_agg sa ON sa.SALESFORCE_ACCOUNT_NAME = r.NAME
  LEFT JOIN si_detail sd ON sd.SALESFORCE_ACCOUNT_NAME = r.NAME
  WHERE r.THEATER = 'AMSExpansion'
    AND r.ACCOUNT_OWNER_FUNCTION_C = 'Account Executive'
  GROUP BY 1, 2, 3, 4, 5
)
SELECT ae, se_name as se, region, district, patch,
       accts_with_si as accts, total_accts as "totAccts",
       ROUND(total_credits, 2) as cr,
       total_users as users, wau, si_questions as "siQ"
FROM base
WHERE accts_with_si > 0
ORDER BY cr DESC
LIMIT 5"""


def q_dm_coco():
    return f"""
WITH {SE_ATTR_CTE},
{COCO_CTE},
base AS (
  SELECT
    r.ACCOUNT_OWNER_MANAGER_C as dm,
    se.ACCOUNT_SE_MANAGER as sem,
    se.REGION_NAME as region,
    LISTAGG(DISTINCT se.DISTRICT_NAME, ', ') WITHIN GROUP (ORDER BY se.DISTRICT_NAME) as districts,
    COUNT(DISTINCT r.ACCOUNT_OWNER_NAME || '|' || se.SALES_ENGINEER_NAME) as ae_se_pairs,
    COUNT(DISTINCT CASE WHEN c.SALESFORCE_ACCOUNT_ID IS NOT NULL THEN r.SALESFORCE_ACCOUNT_ID END) as accts_with_coco,
    COUNT(DISTINCT r.SALESFORCE_ACCOUNT_ID) as total_accts,
    SUM(COALESCE(c.total_credits, 0)) as total_credits,
    SUM(COALESCE(c.total_requests, 0)) as total_requests
  FROM SALES.RAVEN.ACCOUNT r
  JOIN se_attr se ON se.ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
  LEFT JOIN coco c ON c.SALESFORCE_ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
  WHERE r.THEATER = 'AMSExpansion'
    AND r.ACCOUNT_OWNER_FUNCTION_C = 'Account Executive'
    AND r.ACCOUNT_OWNER_MANAGER_C != r.RVP
  GROUP BY 1, 2, 3
)
SELECT dm, sem, region, districts,
       ae_se_pairs as pairs,
       accts_with_coco as accts, total_accts as "totAccts",
       ROUND(total_credits, 2) as cr,
       total_requests as reqs
FROM base
WHERE accts_with_coco > 0
ORDER BY cr DESC
LIMIT 5"""


def q_dm_si():
    return f"""
WITH {SE_ATTR_CTE},
{SI_AGG_CTE},
{SI_DETAIL_CTE},
base AS (
  SELECT
    r.ACCOUNT_OWNER_MANAGER_C as dm,
    se.ACCOUNT_SE_MANAGER as sem,
    se.REGION_NAME as region,
    LISTAGG(DISTINCT se.DISTRICT_NAME, ', ') WITHIN GROUP (ORDER BY se.DISTRICT_NAME) as districts,
    COUNT(DISTINCT r.ACCOUNT_OWNER_NAME || '|' || se.SALES_ENGINEER_NAME) as ae_se_pairs,
    COUNT(DISTINCT CASE WHEN sa.SALESFORCE_ACCOUNT_NAME IS NOT NULL THEN r.NAME END) as accts_with_si,
    COUNT(DISTINCT r.SALESFORCE_ACCOUNT_ID) as total_accts,
    SUM(COALESCE(sa.total_credits, 0)) as total_credits,
    SUM(COALESCE(sa.total_users, 0)) as total_users,
    SUM(COALESCE(sd.si_questions, 0)) as si_questions,
    SUM(COALESCE(sd.wau, 0)) as wau
  FROM SALES.RAVEN.ACCOUNT r
  JOIN se_attr se ON se.ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
  LEFT JOIN si_agg sa ON sa.SALESFORCE_ACCOUNT_NAME = r.NAME
  LEFT JOIN si_detail sd ON sd.SALESFORCE_ACCOUNT_NAME = r.NAME
  WHERE r.THEATER = 'AMSExpansion'
    AND r.ACCOUNT_OWNER_FUNCTION_C = 'Account Executive'
    AND r.ACCOUNT_OWNER_MANAGER_C != r.RVP
  GROUP BY 1, 2, 3
)
SELECT dm, sem, region, districts,
       ae_se_pairs as pairs,
       accts_with_si as accts, total_accts as "totAccts",
       ROUND(total_credits, 2) as cr,
       total_users as users, wau, si_questions as "siQ"
FROM base
WHERE accts_with_si > 0
ORDER BY cr DESC
LIMIT 5"""


def q_rvp_coco():
    return f"""
WITH {SE_ATTR_CTE},
{COCO_CTE},
base AS (
  SELECT
    r.RVP as rvp,
    se.ACCOUNT_SE_DIRECTOR as se_dir,
    se.REGION_NAME as region,
    COUNT(DISTINCT r.ACCOUNT_OWNER_MANAGER_C || '|' || se.ACCOUNT_SE_MANAGER) as dm_pairs,
    COUNT(DISTINCT CASE WHEN c.SALESFORCE_ACCOUNT_ID IS NOT NULL THEN r.SALESFORCE_ACCOUNT_ID END) as accts_with_coco,
    COUNT(DISTINCT r.SALESFORCE_ACCOUNT_ID) as total_accts,
    SUM(COALESCE(c.total_credits, 0)) as total_credits,
    SUM(COALESCE(c.total_requests, 0)) as total_requests
  FROM SALES.RAVEN.ACCOUNT r
  JOIN se_attr se ON se.ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
  LEFT JOIN coco c ON c.SALESFORCE_ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
  WHERE r.THEATER = 'AMSExpansion'
    AND r.ACCOUNT_OWNER_FUNCTION_C = 'Account Executive'
  GROUP BY 1, 2, 3
)
SELECT rvp, se_dir as "seDir", region,
       dm_pairs as "dmPairs",
       accts_with_coco as accts, total_accts as "totAccts",
       ROUND(total_credits, 2) as cr,
       total_requests as reqs
FROM base
WHERE accts_with_coco > 0
ORDER BY cr DESC
LIMIT 5"""


def q_rvp_si():
    return f"""
WITH {SE_ATTR_CTE},
{SI_AGG_CTE},
{SI_DETAIL_CTE},
base AS (
  SELECT
    r.RVP as rvp,
    se.ACCOUNT_SE_DIRECTOR as se_dir,
    se.REGION_NAME as region,
    COUNT(DISTINCT r.ACCOUNT_OWNER_MANAGER_C || '|' || se.ACCOUNT_SE_MANAGER) as dm_pairs,
    COUNT(DISTINCT CASE WHEN sa.SALESFORCE_ACCOUNT_NAME IS NOT NULL THEN r.NAME END) as accts_with_si,
    COUNT(DISTINCT r.SALESFORCE_ACCOUNT_ID) as total_accts,
    SUM(COALESCE(sa.total_credits, 0)) as total_credits,
    SUM(COALESCE(sa.total_users, 0)) as total_users,
    SUM(COALESCE(sd.si_questions, 0)) as si_questions,
    SUM(COALESCE(sd.wau, 0)) as wau
  FROM SALES.RAVEN.ACCOUNT r
  JOIN se_attr se ON se.ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
  LEFT JOIN si_agg sa ON sa.SALESFORCE_ACCOUNT_NAME = r.NAME
  LEFT JOIN si_detail sd ON sd.SALESFORCE_ACCOUNT_NAME = r.NAME
  WHERE r.THEATER = 'AMSExpansion'
    AND r.ACCOUNT_OWNER_FUNCTION_C = 'Account Executive'
  GROUP BY 1, 2, 3
)
SELECT rvp, se_dir as "seDir", region,
       dm_pairs as "dmPairs",
       accts_with_si as accts, total_accts as "totAccts",
       ROUND(total_credits, 2) as cr,
       total_users as users, wau, si_questions as "siQ"
FROM base
WHERE accts_with_si > 0
ORDER BY cr DESC
LIMIT 5"""


def q_summary():
    return f"""
WITH {SE_ATTR_CTE},
{COCO_CTE},
{SI_AGG_CTE}
SELECT
  ROUND(SUM(COALESCE(c.total_credits, 0)), 0) as coco_credits,
  SUM(COALESCE(c.total_requests, 0)) as coco_requests,
  COUNT(DISTINCT CASE WHEN c.SALESFORCE_ACCOUNT_ID IS NOT NULL
        THEN r.ACCOUNT_OWNER_NAME || '|' || se.SALES_ENGINEER_NAME END) as coco_ae_se_pairs,
  ROUND(SUM(COALESCE(sa.total_credits, 0)), 0) as si_credits,
  SUM(COALESCE(sa.total_users, 0)) as si_users
FROM SALES.RAVEN.ACCOUNT r
JOIN se_attr se ON se.ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
LEFT JOIN coco c ON c.SALESFORCE_ACCOUNT_ID = r.SALESFORCE_ACCOUNT_ID
LEFT JOIN si_agg sa ON sa.SALESFORCE_ACCOUNT_NAME = r.NAME
WHERE r.THEATER = 'AMSExpansion'
  AND r.ACCOUNT_OWNER_FUNCTION_C = 'Account Executive'"""


# ---------------------------------------------------------------------------
# JSON serialiser helper
# ---------------------------------------------------------------------------

def to_js_array(rows: list[dict]) -> str:
    """Convert query results to a JS array literal, handling Decimal/date types.

    Snowflake returns unquoted column aliases as UPPERCASE, but our JS renderers
    expect the exact casing from the SQL AS clause (e.g. ``d.ae``, ``d.totAccts``).
    We build a mapping from the original SQL aliases so every key is emitted with
    the intended case.
    """
    # Expected JS key names – map UPPER to intended case
    _CASE_MAP = {
        "AE": "ae", "SE": "se", "REGION": "region", "DISTRICT": "district",
        "PATCH": "patch", "ACCTS": "accts", "CR": "cr", "REQS": "reqs",
        "DM": "dm", "SEM": "sem", "DISTRICTS": "districts", "PAIRS": "pairs",
        "USERS": "users", "WAU": "wau", "RVP": "rvp",
        # Quoted aliases already come back with correct case
        "totAccts": "totAccts", "seDir": "seDir", "dmPairs": "dmPairs", "siQ": "siQ",
    }

    clean = []
    for row in rows:
        obj = {}
        for k, v in row.items():
            key = _CASE_MAP.get(k, k)  # fix case; pass through unknown keys as-is
            if v is None:
                obj[key] = ""
            elif isinstance(v, (int, float)):
                obj[key] = v
            else:
                # Convert Decimal, date, etc. to native types
                try:
                    obj[key] = float(v)
                    if obj[key] == int(obj[key]):
                        obj[key] = int(obj[key])
                except (ValueError, TypeError):
                    obj[key] = str(v)
        clean.append(obj)
    return json.dumps(clean, ensure_ascii=False)


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

def build_html(data: dict, summary: dict, gen_date: str) -> str:
    """Build the full self-contained HTML string."""

    coco_90d_label = f"{(date.today() - timedelta(days=90)).strftime('%b %Y')}&ndash;{date.today().strftime('%b %Y')}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AMSExpansion Adoption Leaderboard - Cortex Code & Snowflake Intelligence</title>
<style>
  :root {{
    --snow-blue: #0056b3;
    --snow-light: #29b5e8;
    --snow-dark: #003d82;
    --gold: #f5a623;
    --silver: #b0b0b0;
    --bronze: #cd7f32;
    --bg: #f4f7fb;
    --card-bg: #ffffff;
    --text: #1e293b;
    --muted: #64748b;
    --border: #e2e8f0;
    --success: #10b981;
    --coco-color: #8b5cf6;
    --si-color: #0ea5e9;
  }}

  * {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.5;
  }}

  .header {{
    background: linear-gradient(135deg, var(--snow-dark), var(--snow-blue), var(--snow-light));
    color: white;
    padding: 32px 40px;
    position: relative;
    overflow: hidden;
  }}
  .header::after {{
    content: '';
    position: absolute;
    top: -50%;
    right: -10%;
    width: 400px;
    height: 400px;
    background: rgba(255,255,255,0.05);
    border-radius: 50%;
  }}
  .header h1 {{ font-size: 28px; font-weight: 700; margin-bottom: 4px; }}
  .header .subtitle {{ font-size: 15px; opacity: 0.85; }}
  .header .meta {{ font-size: 12px; opacity: 0.65; margin-top: 8px; }}

  .container {{ max-width: 1600px; margin: 0 auto; padding: 24px; }}

  .summary-cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 16px;
    margin-bottom: 32px;
  }}
  .summary-card {{
    background: var(--card-bg);
    border-radius: 10px;
    padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    text-align: center;
  }}
  .summary-card .value {{
    font-size: 28px;
    font-weight: 700;
    color: var(--snow-blue);
  }}
  .summary-card .value.coco {{ color: var(--coco-color); }}
  .summary-card .value.si {{ color: var(--si-color); }}
  .summary-card .label {{
    font-size: 12px;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-top: 4px;
  }}

  .data-notes {{
    background: var(--card-bg);
    border-radius: 10px;
    padding: 16px 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    margin-bottom: 28px;
    font-size: 12px;
    color: var(--muted);
    line-height: 1.7;
  }}
  .data-notes strong {{ color: var(--text); }}

  .section {{
    background: var(--card-bg);
    border-radius: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    margin-bottom: 28px;
    overflow: hidden;
  }}
  .section-header {{
    padding: 18px 24px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    gap: 10px;
  }}
  .section-header h2 {{ font-size: 18px; font-weight: 600; }}
  .section-header .badge {{
    font-size: 11px;
    padding: 2px 8px;
    border-radius: 10px;
    font-weight: 600;
  }}
  .badge-si {{ background: #e0f2fe; color: #0369a1; }}
  .badge-coco {{ background: #ede9fe; color: #6d28d9; }}
  .section-header .note {{ font-size: 12px; color: var(--muted); margin-left: auto; }}

  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    table-layout: auto;
  }}
  thead th {{
    background: #f8fafc;
    padding: 8px 10px;
    text-align: left;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--muted);
    border-bottom: 2px solid var(--border);
    white-space: nowrap;
  }}
  thead th.right {{ text-align: right; }}
  tbody td {{
    padding: 8px 10px;
    border-bottom: 1px solid var(--border);
    vertical-align: middle;
    white-space: nowrap;
  }}
  tbody td.right {{ text-align: right; font-variant-numeric: tabular-nums; }}
  tbody td.geo {{ font-size: 12px; color: var(--muted); white-space: nowrap; }}
  tbody tr:hover {{ background: #f8fafc; }}
  tbody tr:last-child td {{ border-bottom: none; }}

  .rank {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    border-radius: 50%;
    font-weight: 700;
    font-size: 12px;
    color: white;
  }}
  .rank-1 {{ background: var(--gold); }}
  .rank-2 {{ background: var(--silver); }}
  .rank-3 {{ background: var(--bronze); }}
  .rank-n {{ background: #cbd5e1; color: #475569; font-weight: 500; }}

  .pair-names {{ line-height: 1.3; }}
  .pair-names .primary {{ font-weight: 600; color: var(--text); }}
  .pair-names .secondary {{ font-size: 12px; color: var(--muted); }}

  .bar-cell {{ min-width: 80px; width: 80px; }}
  .bar-wrapper {{ display: flex; align-items: center; gap: 8px; }}
  .bar {{
    height: 8px;
    border-radius: 4px;
    min-width: 2px;
    transition: width 0.3s;
  }}
  .bar-si {{ background: linear-gradient(90deg, #0ea5e9, #38bdf8); }}
  .bar-coco {{ background: linear-gradient(90deg, #8b5cf6, #a78bfa); }}
  .bar-total {{ background: linear-gradient(90deg, var(--snow-blue), var(--snow-light)); }}
  .bar-value {{ font-size: 12px; color: var(--muted); white-space: nowrap; }}

  .pct {{ font-weight: 600; }}
  .pct-high {{ color: var(--success); }}
  .pct-mid {{ color: var(--gold); }}
  .pct-low {{ color: var(--muted); }}

  .tab-container {{ display: flex; gap: 4px; padding: 0 24px; padding-top: 12px; }}
  .tab {{
    padding: 8px 16px;
    border: none;
    background: transparent;
    color: var(--muted);
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    border-bottom: 2px solid transparent;
    transition: all 0.2s;
  }}
  .tab:hover {{ color: var(--text); }}
  .tab.active {{ color: var(--snow-blue); border-bottom-color: var(--snow-blue); font-weight: 700; }}

  .tab-panel {{ display: none; overflow-x: auto; }}
  .tab-panel.active {{ display: block; }}

  .footer {{
    text-align: center;
    padding: 20px;
    font-size: 11px;
    color: var(--muted);
  }}

  @media print {{
    .header {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    .tab-panel {{ display: block !important; }}
    .tab-container {{ display: none; }}
  }}
</style>
</head>
<body>

<div class="header">
  <h1>Cortex Code & Snowflake Intelligence Adoption Leaderboard</h1>
  <div class="subtitle">AMSExpansion Theater &mdash; Top 5 Groupings</div>
  <div class="meta">Generated {gen_date} &nbsp;|&nbsp; SI Data: Last 90 Days &nbsp;|&nbsp; CoCo Data: Last 90 Days ({coco_90d_label}) &nbsp;|&nbsp; Sources: Internal Telemetry, SFDC, SE Attributes</div>
</div>

<div class="container">

  <!-- Summary Cards -->
  <div class="summary-cards">
    <div class="summary-card">
      <div class="value si">{summary['si_credits']:,}</div>
      <div class="label">SI Credits (90d)</div>
    </div>
    <div class="summary-card">
      <div class="value si">{summary['si_users']:,}</div>
      <div class="label">SI Users (90d)</div>
    </div>
    <div class="summary-card">
      <div class="value coco">{summary['coco_credits']:,}</div>
      <div class="label">CoCo Credits (90d)</div>
    </div>
    <div class="summary-card">
      <div class="value coco">{summary['coco_requests']:,}</div>
      <div class="label">CoCo Requests (90d)</div>
    </div>
    <div class="summary-card">
      <div class="value">{summary['coco_ae_se_pairs']:,}</div>
      <div class="label">AE/SE Pairs Active</div>
    </div>
  </div>

  <!-- AE / SE PAIR LEADERBOARD -->
  <div class="section">
    <div class="section-header">
      <h2>AE / SE Pair Leaderboard</h2>
      <span class="badge badge-coco">Top 5</span>
      <span class="note">Ranked by Credits</span>
    </div>
    <div class="tab-container">
      <button class="tab active" onclick="switchTab(event, 'ae-coco')">CoCo Only</button>
      <button class="tab" onclick="switchTab(event, 'ae-si')">SI Only</button>
    </div>
    <div id="ae-coco" class="tab-panel active">
      <table>
        <thead><tr>
          <th style="width:40px">#</th><th>AE / SE</th><th>Region</th><th>District</th><th>Patch</th>
          <th class="right">Accts Using</th><th class="right">Total Accts</th><th class="right">% CoCo</th>
          <th class="right">CoCo Credits</th><th class="right">CoCo Requests</th><th class="bar-cell">Credits</th>
        </tr></thead>
        <tbody id="ae-coco-body"></tbody>
      </table>
    </div>
    <div id="ae-si" class="tab-panel">
      <table>
        <thead><tr>
          <th style="width:40px">#</th><th>AE / SE</th><th>Region</th><th>District</th><th>Patch</th>
          <th class="right">Accts Using</th><th class="right">Total Accts</th><th class="right">% SI</th>
          <th class="right">SI Credits (90d)</th><th class="right">SI Users (90d)</th>
          <th class="right">SI WAU</th><th class="right">SI Questions (90d)</th><th class="bar-cell">Credits</th>
        </tr></thead>
        <tbody id="ae-si-body"></tbody>
      </table>
    </div>
  </div>

  <!-- DM / SEM PAIR LEADERBOARD -->
  <div class="section">
    <div class="section-header">
      <h2>DM / SEM Pair Leaderboard</h2>
      <span class="badge badge-coco">Top 5</span>
      <span class="note">Ranked by Credits</span>
    </div>
    <div class="tab-container">
      <button class="tab active" onclick="switchTab(event, 'dm-coco')">CoCo Only</button>
      <button class="tab" onclick="switchTab(event, 'dm-si')">SI Only</button>
    </div>
    <div id="dm-coco" class="tab-panel active">
      <table>
        <thead><tr>
          <th style="width:40px">#</th><th>DM / SEM</th><th>Region</th><th>District</th>
          <th class="right">AE/SE Pairs</th><th class="right">Accts Using</th><th class="right">Total Accts</th>
          <th class="right">% CoCo</th><th class="right">CoCo Credits</th><th class="right">CoCo Requests</th>
          <th class="bar-cell">Credits</th>
        </tr></thead>
        <tbody id="dm-coco-body"></tbody>
      </table>
    </div>
    <div id="dm-si" class="tab-panel">
      <table>
        <thead><tr>
          <th style="width:40px">#</th><th>DM / SEM</th><th>Region</th><th>District</th>
          <th class="right">AE/SE Pairs</th><th class="right">Accts Using</th><th class="right">Total Accts</th>
          <th class="right">% SI</th><th class="right">SI Credits (90d)</th><th class="right">SI Users (90d)</th>
          <th class="right">SI WAU</th><th class="right">SI Questions (90d)</th><th class="bar-cell">Credits</th>
        </tr></thead>
        <tbody id="dm-si-body"></tbody>
      </table>
    </div>
  </div>

  <!-- RVP / SE DIRECTOR LEADERBOARD -->
  <div class="section">
    <div class="section-header">
      <h2>RVP / SE Director Leaderboard</h2>
      <span class="badge badge-coco">Region Level</span>
      <span class="note">Ranked by Credits</span>
    </div>
    <div class="tab-container">
      <button class="tab active" onclick="switchTab(event, 'rvp-coco')">CoCo Only</button>
      <button class="tab" onclick="switchTab(event, 'rvp-si')">SI Only</button>
    </div>
    <div id="rvp-coco" class="tab-panel active">
      <table>
        <thead><tr>
          <th style="width:40px">#</th><th>RVP / SE Director</th><th>Region</th>
          <th class="right">DM/SEM Pairs</th><th class="right">Accts Using</th><th class="right">Total Accts</th>
          <th class="right">% CoCo</th><th class="right">CoCo Credits</th><th class="right">CoCo Requests</th>
          <th class="bar-cell">Credits</th>
        </tr></thead>
        <tbody id="rvp-coco-body"></tbody>
      </table>
    </div>
    <div id="rvp-si" class="tab-panel">
      <table>
        <thead><tr>
          <th style="width:40px">#</th><th>RVP / SE Director</th><th>Region</th>
          <th class="right">DM/SEM Pairs</th><th class="right">Accts Using</th><th class="right">Total Accts</th>
          <th class="right">% SI</th><th class="right">SI Credits (90d)</th><th class="right">SI Users (90d)</th>
          <th class="right">SI WAU</th><th class="right">SI Questions (90d)</th><th class="bar-cell">Credits</th>
        </tr></thead>
        <tbody id="rvp-si-body"></tbody>
      </table>
    </div>
  </div>

  <!-- Data Definitions -->
  <div class="data-notes">
    <strong>Data Definitions:</strong><br>
    <strong>Accts Using</strong> = Number of accounts with product activity (CoCo or SI) in the pair's portfolio.<br>
    <strong>Total Accts</strong> = Total accounts assigned to the AE/SE or DM/SEM pair in AMSExpansion.<br>
    <strong>% CoCo / % SI</strong> = Adoption rate &mdash; Accts Using / Total Accts.<br>
    <strong>SI Users (90d)</strong> = Distinct users with Snowflake Intelligence activity in the last 90 days.<br>
    <strong>SI WAU</strong> = Weekly Active Users &mdash; distinct SI users in the last 7 days.<br>
    <strong>SI Credits</strong> = Token credits consumed by SI queries in the 90-day window.<br>
    <strong>SI Questions</strong> = Distinct requests (queries) made to Snowflake Intelligence.<br>
    <strong>CoCo Credits</strong> = Cortex Code credits consumed in the last 90 days. Includes CLI, Desktop, and UI usage.<br>
    <strong>Region / District / Patch</strong> = From SE Attributes hierarchy.<br>
    <strong>RVP</strong> = Regional Vice President (sales). <strong>SE Director</strong> = SE leadership counterpart.<br>
    <strong>Open Position Exclusion</strong> = Only accounts owned by active Account Executives are included.
  </div>

  <div class="footer">
    Data Sources: SNOWSCIENCE.LLM.CORTEX_CODE_REQUEST_FACT,
    SALES.DEV.BOB_SNOWFLAKE_INTELLIGENCE_USAGE_STREAMLIT_AGG,
    SNOWSCIENCE.LLM.SNOWFLAKE_INTELLIGENCE_ACCOUNTS_CREDITS,
    SALES.RAVEN.ACCOUNT,
    SALES.SE_REPORTING.DIM_ACCOUNTS_SE_ATTRIBUTES_DAILY
  </div>
</div>

<script>
// ---- DATA ----
const aeCocoOnly = {data['ae_coco']};
const aeSiOnly = {data['ae_si']};
const dmCocoOnly = {data['dm_coco']};
const dmSiOnly = {data['dm_si']};
const rvpCocoOnly = {data['rvp_coco']};
const rvpSiOnly = {data['rvp_si']};

// ---- RENDERING ----
function rankBadge(i) {{
  const cls = i < 3 ? `rank-${{i+1}}` : 'rank-n';
  return `<span class="rank ${{cls}}">${{i+1}}</span>`;
}}
function fmt(n) {{ return n == null ? '—' : n.toLocaleString('en-US', {{minimumFractionDigits:0, maximumFractionDigits:2}}); }}
function pairCell(a, b) {{
  return `<div class="pair-names"><div class="primary">${{a || '(Vacant)'}}</div><div class="secondary">${{b || '(Unmapped)'}}</div></div>`;
}}
function singleBar(val, max, cls) {{
  const pct = max > 0 ? (val/max)*100 : 0;
  return `<div class="bar-wrapper"><div class="bar ${{cls}}" style="width:${{Math.max(pct,1)}}%"></div></div>`;
}}
function pctCell(accts, total) {{
  if (!total || total === 0) return '<td class="right">&mdash;</td>';
  const pct = Math.round(accts / total * 100);
  const cls = pct >= 50 ? 'pct-high' : pct >= 20 ? 'pct-mid' : 'pct-low';
  return `<td class="right"><span class="pct ${{cls}}">${{pct}}%</span></td>`;
}}

function renderAeCocoSingle(tbody, data) {{
  const max = Math.max(...data.map(d => d.cr));
  tbody.innerHTML = data.map((d, i) => `<tr>
    <td>${{rankBadge(i)}}</td>
    <td>${{pairCell(d.ae, d.se)}}</td>
    <td class="geo">${{d.region || '—'}}</td>
    <td class="geo">${{d.district || '—'}}</td>
    <td class="geo">${{d.patch || '—'}}</td>
    <td class="right">${{d.accts}}</td>
    <td class="right">${{d.totAccts}}</td>
    ${{pctCell(d.accts, d.totAccts)}}
    <td class="right"><strong>${{fmt(d.cr)}}</strong></td>
    <td class="right">${{fmt(d.reqs)}}</td>
    <td class="bar-cell">${{singleBar(d.cr, max, 'bar-coco')}}</td>
  </tr>`).join('');
}}

function renderAeSiSingle(tbody, data) {{
  const max = Math.max(...data.map(d => d.cr));
  tbody.innerHTML = data.map((d, i) => `<tr>
    <td>${{rankBadge(i)}}</td>
    <td>${{pairCell(d.ae, d.se)}}</td>
    <td class="geo">${{d.region || '—'}}</td>
    <td class="geo">${{d.district || '—'}}</td>
    <td class="geo">${{d.patch || '—'}}</td>
    <td class="right">${{d.accts}}</td>
    <td class="right">${{d.totAccts}}</td>
    ${{pctCell(d.accts, d.totAccts)}}
    <td class="right"><strong>${{fmt(d.cr)}}</strong></td>
    <td class="right">${{fmt(d.users)}}</td>
    <td class="right">${{fmt(d.wau)}}</td>
    <td class="right">${{fmt(d.siQ)}}</td>
    <td class="bar-cell">${{singleBar(d.cr, max, 'bar-si')}}</td>
  </tr>`).join('');
}}

function renderDmCocoSingle(tbody, data) {{
  const max = Math.max(...data.map(d => d.cr));
  tbody.innerHTML = data.map((d, i) => `<tr>
    <td>${{rankBadge(i)}}</td>
    <td>${{pairCell(d.dm, d.sem)}}</td>
    <td class="geo">${{d.region || '—'}}</td>
    <td class="geo">${{d.districts || '—'}}</td>
    <td class="right">${{d.pairs}}</td>
    <td class="right">${{d.accts}}</td>
    <td class="right">${{d.totAccts}}</td>
    ${{pctCell(d.accts, d.totAccts)}}
    <td class="right"><strong>${{fmt(d.cr)}}</strong></td>
    <td class="right">${{fmt(d.reqs)}}</td>
    <td class="bar-cell">${{singleBar(d.cr, max, 'bar-coco')}}</td>
  </tr>`).join('');
}}

function renderDmSiSingle(tbody, data) {{
  const max = Math.max(...data.map(d => d.cr));
  tbody.innerHTML = data.map((d, i) => `<tr>
    <td>${{rankBadge(i)}}</td>
    <td>${{pairCell(d.dm, d.sem)}}</td>
    <td class="geo">${{d.region || '—'}}</td>
    <td class="geo">${{d.districts || '—'}}</td>
    <td class="right">${{d.pairs}}</td>
    <td class="right">${{d.accts}}</td>
    <td class="right">${{d.totAccts}}</td>
    ${{pctCell(d.accts, d.totAccts)}}
    <td class="right"><strong>${{fmt(d.cr)}}</strong></td>
    <td class="right">${{fmt(d.users)}}</td>
    <td class="right">${{fmt(d.wau)}}</td>
    <td class="right">${{fmt(d.siQ)}}</td>
    <td class="bar-cell">${{singleBar(d.cr, max, 'bar-si')}}</td>
  </tr>`).join('');
}}

function renderRvpCocoSingle(tbody, data) {{
  const max = Math.max(...data.map(d => d.cr));
  tbody.innerHTML = data.map((d, i) => `<tr>
    <td>${{rankBadge(i)}}</td>
    <td>${{pairCell(d.rvp, d.seDir)}}</td>
    <td class="geo">${{d.region || '—'}}</td>
    <td class="right">${{d.dmPairs}}</td>
    <td class="right">${{d.accts}}</td>
    <td class="right">${{d.totAccts}}</td>
    ${{pctCell(d.accts, d.totAccts)}}
    <td class="right"><strong>${{fmt(d.cr)}}</strong></td>
    <td class="right">${{fmt(d.reqs)}}</td>
    <td class="bar-cell">${{singleBar(d.cr, max, 'bar-coco')}}</td>
  </tr>`).join('');
}}

function renderRvpSiSingle(tbody, data) {{
  const max = Math.max(...data.map(d => d.cr));
  tbody.innerHTML = data.map((d, i) => `<tr>
    <td>${{rankBadge(i)}}</td>
    <td>${{pairCell(d.rvp, d.seDir)}}</td>
    <td class="geo">${{d.region || '—'}}</td>
    <td class="right">${{d.dmPairs}}</td>
    <td class="right">${{d.accts}}</td>
    <td class="right">${{d.totAccts}}</td>
    ${{pctCell(d.accts, d.totAccts)}}
    <td class="right"><strong>${{fmt(d.cr)}}</strong></td>
    <td class="right">${{fmt(d.users)}}</td>
    <td class="right">${{fmt(d.wau)}}</td>
    <td class="right">${{fmt(d.siQ)}}</td>
    <td class="bar-cell">${{singleBar(d.cr, max, 'bar-si')}}</td>
  </tr>`).join('');
}}

// Render all tables
renderAeCocoSingle(document.getElementById('ae-coco-body'), aeCocoOnly);
renderAeSiSingle(document.getElementById('ae-si-body'), aeSiOnly);
renderDmCocoSingle(document.getElementById('dm-coco-body'), dmCocoOnly);
renderDmSiSingle(document.getElementById('dm-si-body'), dmSiOnly);
renderRvpCocoSingle(document.getElementById('rvp-coco-body'), rvpCocoOnly);
renderRvpSiSingle(document.getElementById('rvp-si-body'), rvpSiOnly);

// Tab switching
function switchTab(e, panelId) {{
  const section = e.target.closest('.section');
  section.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  section.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  e.target.classList.add('active');
  document.getElementById(panelId).classList.add('active');
}}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate AMSExpansion Adoption Leaderboard HTML from Snowflake data."
    )
    parser.add_argument(
        "--connection", "-c",
        default="MyConnection",
        help="Snowflake connection name from ~/.snowflake/connections.toml (default: MyConnection)",
    )
    parser.add_argument(
        "--warehouse", "-w",
        default="CORPORATE_SE_WH",
        help="Snowflake warehouse to use (default: CORPORATE_SE_WH)",
    )
    parser.add_argument(
        "--output", "-o",
        default="AMSExpansion_Adoption_Leaderboard.html",
        help="Output HTML filename (default: AMSExpansion_Adoption_Leaderboard.html)",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Don't open the HTML file in the browser after generating",
    )
    args = parser.parse_args()

    print(f"Connecting to Snowflake (connection: {args.connection})...")
    conn = get_connection(args.connection, args.warehouse)
    print("Connected.\n")

    queries = {
        "ae_coco":  ("AE/SE CoCo Top 5",  q_ae_coco()),
        "ae_si":    ("AE/SE SI Top 5",     q_ae_si()),
        "dm_coco":  ("DM/SEM CoCo Top 5",  q_dm_coco()),
        "dm_si":    ("DM/SEM SI Top 5",     q_dm_si()),
        "rvp_coco": ("RVP/SE Dir CoCo Top 5", q_rvp_coco()),
        "rvp_si":   ("RVP/SE Dir SI Top 5",   q_rvp_si()),
        "summary":  ("Summary totals",      q_summary()),
    }

    results = {}
    for key, (label, sql) in queries.items():
        print(f"  Querying {label}...")
        results[key] = run_query(conn, sql)

    conn.close()
    print("\nAll queries complete. Generating HTML...")

    # Build data dict for JS arrays
    data = {}
    for key in ["ae_coco", "ae_si", "dm_coco", "dm_si", "rvp_coco", "rvp_si"]:
        data[key] = to_js_array(results[key])

    # Summary values
    s = results["summary"][0] if results["summary"] else {}
    summary = {
        "si_credits":       int(s.get("SI_CREDITS", 0) or 0),
        "si_users":         int(s.get("SI_USERS", 0) or 0),
        "coco_credits":     int(s.get("COCO_CREDITS", 0) or 0),
        "coco_requests":    int(s.get("COCO_REQUESTS", 0) or 0),
        "coco_ae_se_pairs": int(s.get("COCO_AE_SE_PAIRS", 0) or 0),
    }

    gen_date = date.today().strftime("%B %-d, %Y")
    html = build_html(data, summary, gen_date)

    output_path = Path(args.output).resolve()
    output_path.write_text(html, encoding="utf-8")
    print(f"\nLeaderboard saved to: {output_path}")

    if not args.no_open:
        print("Opening in browser...")
        webbrowser.open(f"file://{output_path}")

    print("Done.")


if __name__ == "__main__":
    main()
