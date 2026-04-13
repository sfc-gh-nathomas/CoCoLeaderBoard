#!/usr/bin/env python3
"""
leaderboard_app.py  —  AMSExpansion Pipeline Leaderboard
Streamlit app styled to match the HTML leaderboard format.
"""

import os
import math
import time as _time
import threading
import concurrent.futures
import html as _html_lib
import streamlit as st
import pandas as pd
from datetime import date

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AMSExpansion Pipeline Leaderboard",
    page_icon=":material/leaderboard:",
    layout="wide",
)

# ── CSS — mirrors the HTML leaderboard stylesheet ─────────────────────────────
st.markdown("""
<style>
  :root {
    --snow-blue: #0056b3; --snow-light: #29b5e8; --snow-dark: #003d82;
    --gold: #f5a623; --silver: #b0b0b0; --bronze: #cd7f32;
    --bg: #f4f7fb; --card-bg: #ffffff; --text: #1e293b;
    --muted: #64748b; --border: #e2e8f0;
    --success: #10b981; --danger: #ef4444;
  }
  /* hide streamlit chrome for a cleaner look */
  [data-testid="stAppViewContainer"] { background: var(--bg); }
  [data-testid="stHeader"] { display: none; }
  .block-container { padding-top: 0 !important; max-width: 1600px; }

  /* ── header ── */
  .lb-header {
    background: linear-gradient(135deg, var(--snow-dark), var(--snow-blue), var(--snow-light));
    color: white; padding: 28px 36px; border-radius: 0 0 8px 8px; margin-bottom: 20px;
  }
  .lb-header h1 { font-size: 24px; font-weight: 700; margin: 0 0 4px; }
  .lb-header .subtitle { font-size: 14px; opacity: 0.85; }

  /* ── summary cards ── */
  .summary-grid { display: flex; gap: 14px; margin-bottom: 24px; flex-wrap: wrap; }
  .scard {
    background: var(--card-bg); border-radius: 10px; padding: 16px 18px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08); text-align: center; flex: 1; min-width: 140px;
  }
  .scard .val { font-size: 24px; font-weight: 700; color: var(--snow-blue); }
  .scard .sub { font-size: 11px; color: var(--muted); font-weight: 600; margin-top: 2px; }
  .scard .lbl { font-size: 11px; color: var(--muted); text-transform: uppercase;
                 letter-spacing: 0.5px; margin-top: 6px; }

  /* ── group label ── */
  .group-label {
    font-size: 13px; font-weight: 700; color: var(--snow-dark); text-transform: uppercase;
    letter-spacing: 0.8px; margin: 24px 0 10px; padding-bottom: 6px;
    border-bottom: 2px solid var(--snow-light);
  }

  /* ── section cards ── */
  .lb-section {
    background: var(--card-bg); border-radius: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-bottom: 16px; overflow: hidden;
  }
  .section-header {
    padding: 14px 18px; border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 10px;
  }
  .section-header h2 { font-size: 15px; font-weight: 600; margin: 0; }
  .badge {
    font-size: 11px; padding: 2px 8px; border-radius: 10px; font-weight: 600;
    background: #dbeafe; color: #1d4ed8;
  }
  .snote { font-size: 12px; color: var(--muted); margin-left: auto; }

  /* ── tables ── */
  .lb-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .lb-table thead th {
    background: #f8fafc; padding: 7px 10px; text-align: left; font-weight: 600;
    font-size: 11px; text-transform: uppercase; letter-spacing: 0.4px; color: var(--muted);
    border-bottom: 2px solid var(--border); white-space: nowrap;
  }
  .lb-table thead th.right { text-align: right; }
  .lb-table tbody td {
    padding: 7px 10px; border-bottom: 1px solid var(--border); vertical-align: middle;
  }
  .lb-table tbody td.right { text-align: right; }
  .lb-table tbody td.mono { font-variant-numeric: tabular-nums; }
  .lb-table tbody td.muted { color: var(--muted); font-size: 12px; }
  .lb-table tbody td.small { font-size: 11px; }
  .lb-table tbody td.primary { font-weight: 600; }
  .lb-table tbody tr:hover { background: #f8fafc; }
  .lb-table tbody tr:last-child td { border-bottom: none; }
  .empty-row { color: var(--muted); font-size: 13px; padding: 14px 10px; }

  /* ── rank badges ── */
  .rank {
    display: inline-flex; align-items: center; justify-content: center;
    width: 24px; height: 24px; border-radius: 50%; font-weight: 700;
    font-size: 11px; color: white;
  }
  .rank-1 { background: var(--gold); }
  .rank-2 { background: var(--silver); }
  .rank-3 { background: var(--bronze); }
  .rank-n { background: #cbd5e1; color: #475569; font-weight: 500; }

  /* ── pair names ── */
  .pair-primary { font-weight: 600; }
  .pair-secondary { font-size: 11px; color: var(--muted); }

  /* ── progress bars ── */
  .bar-cell { min-width: 90px; width: 90px; }
  .bar-wrapper { display: flex; align-items: center; }
  .bar { height: 6px; border-radius: 3px; min-width: 3px; }
  .bar-total { background: linear-gradient(90deg, var(--snow-blue), var(--snow-light)); }
  .bar-meeting { background: linear-gradient(90deg, #10b981, #34d399); }

  /* ── pct colours ── */
  .pct-pos { color: var(--success); font-weight: 600; }
  .pct-neg { color: var(--danger); font-weight: 600; }
  .pct-neu { color: var(--muted); }

  /* streamlit tab style override */
  .stTabs [data-baseweb="tab-list"] { gap: 4px; }
  .stTabs [data-baseweb="tab"] { padding: 6px 14px; font-size: 13px; font-weight: 500; }
</style>
""", unsafe_allow_html=True)


# ── Constants ─────────────────────────────────────────────────────────────────
_LOOKBACK             = "'2025-11-01'"   # Snowflake FY26 Q4 start (Nov 1 2025)
_LOOKBACK_CONSUMPTION = "'2024-10-01'"   # 13 months before _LOOKBACK — needed for YoY LAGs

_REGIONS = [
    "Theater", "SoutheastExp", "NortheastExp", "CentralExp",
    "CanadaExp", "Commercial", "USGrowthExp", "NorthwestExp", "SouthwestExp",
]

_SE_ATTR_CTE = """se_attr AS (
    SELECT ACCOUNT_ID, SALES_ENGINEER_NAME
    FROM SNOWPUBLIC.STREAMLIT.DIM_ACCOUNTS_SLIM_CACHE
    WHERE DS = (SELECT MAX(DS) FROM SNOWPUBLIC.STREAMLIT.DIM_ACCOUNTS_SLIM_CACHE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID
                               ORDER BY SALES_ENGINEER_NAME NULLS LAST) = 1
)"""


# ── Period helpers ────────────────────────────────────────────────────────────
def _period_options():
    today = date.today()
    cutoff = date(2025, 11, 1)  # Snowflake FY26 Q4 start — don't show earlier periods
    options, quarters_added = [], set()
    # Fiscal quarter triggered at its LAST month: FQ1→Apr, FQ2→Jul, FQ3→Oct, FQ4→Jan
    _fq_last = {4: 1, 7: 2, 10: 3, 1: 4}
    for i in range(24):
        m, y = today.month - i, today.year
        while m <= 0:
            m += 12; y -= 1
        if date(y, m, 1) < cutoff:
            break
        options.append(f"{y:04d}-{m:02d}")
        if m in _fq_last:
            # FY label: months Feb-Dec → year+1; Jan → same year
            fy_2d = ((y + 1) % 100) if m >= 2 else (y % 100)
            fq    = _fq_last[m]
            q_key = f"FY{fy_2d:02d}-Q{fq}"
            # Don't add if the quarter's last month is still in the future (include in-progress)
            if q_key not in quarters_added and not (y == today.year and m > today.month):
                quarters_added.add(q_key)
                options.append(q_key)
    return options

def _default_period():
    today = date.today()
    m, y = today.month - 1, today.year
    if m == 0: m, y = 12, y - 1
    return f"{y:04d}-{m:02d}"


# ── Snowflake connection ───────────────────────────────────────────────────────
def _is_in_snowflake():
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session(); return True
    except Exception:
        return False

# Evaluated once at startup — never changes within a session
_IN_SNOWFLAKE: bool = _is_in_snowflake()

# ── Module-level cache — survives Streamlit re-runs, populated by worker threads ──
_QUERY_CACHE: dict = {}
_QUERY_CACHE_LOCK = threading.Lock()
_CACHE_TTL = 1800  # seconds

# Per-thread Snowflake connections (worker threads can't share st.session_state)
_thread_local = threading.local()

def _get_thread_conn():
    """Return a Snowflake connector connection that belongs to the calling thread."""
    if not hasattr(_thread_local, "conn") or _thread_local.conn is None:
        import snowflake.connector
        _thread_local.conn = snowflake.connector.connect(
            connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "MyConnection"
        )
        _thread_local.conn.cursor().execute("USE WAREHOUSE SALES_STREAMLIT_WH")
    return _thread_local.conn

def _execute_sql(sql: str) -> pd.DataFrame:
    """Run SQL on whatever execution context is available for this thread.
    Always tries get_active_session() first at runtime (not the module-load-time
    _IN_SNOWFLAKE flag) so SiS works even if the session wasn't ready at import."""
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        df = session.sql(sql).to_pandas()
        df.columns = [c.upper() for c in df.columns]
        return df
    except Exception:
        pass
    # Fall back to snowflake.connector (local development)
    conn = _get_thread_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0].upper() for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()

def _query_all(sql: str) -> pd.DataFrame:
    """Fetch all regions + periods. Cached at module level so worker threads can populate it."""
    now = _time.monotonic()
    with _QUERY_CACHE_LOCK:
        if sql in _QUERY_CACHE:
            df, ts = _QUERY_CACHE[sql]
            if now - ts < _CACHE_TTL:
                return df
    df = _execute_sql(sql)
    now = _time.monotonic()
    with _QUERY_CACHE_LOCK:
        # Re-check: another thread may have populated the cache while we were executing
        if sql in _QUERY_CACHE:
            cached_df, ts = _QUERY_CACHE[sql]
            if now - ts < _CACHE_TTL:
                return cached_df
        _QUERY_CACHE[sql] = (df, now)
    return df

def _query(sql: str, region: str, periods: list) -> pd.DataFrame:
    """Filter the cached full result to the selected region + periods (pure Python — no DB call)."""
    df = _query_all(sql)
    if df.empty:
        return df
    return df[df["REGION"].eq(region) & df["PERIOD_KEY"].isin(periods)].reset_index(drop=True)

def _preload_parallel(sqls: list):
    """Fire all queries concurrently and populate the module cache.
    Falls back to sequential inside Snowflake SiS (no extra threads available).
    Query failures are cached as empty DataFrames so the app degrades gracefully
    rather than crashing at startup on permission errors."""
    # Runtime check — same pattern as _execute_sql so we never misdetect SiS.
    _in_sis = False
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session()
        _in_sis = True
    except Exception:
        pass
    if _in_sis:
        for sql in sqls:
            try:
                _query_all(sql)
            except Exception:
                with _QUERY_CACHE_LOCK:
                    _QUERY_CACHE[sql] = (pd.DataFrame(), _time.monotonic())
        return
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_query_all, sql): sql for sql in sqls}
        for fut in concurrent.futures.as_completed(futures):
            try:
                fut.result()
            except Exception:
                failed_sql = futures[fut]
                with _QUERY_CACHE_LOCK:
                    _QUERY_CACHE[failed_sql] = (pd.DataFrame(), _time.monotonic())

def _clear_cache():
    with _QUERY_CACHE_LOCK:
        _QUERY_CACHE.clear()


# ── SQL micro-helpers ─────────────────────────────────────────────────────────
def _mk(e): return f"TO_CHAR(DATE_TRUNC('month', {e}), 'YYYY-MM')"
def _qk(e):
    # Snowflake fiscal year starts Feb 1.
    # FY label = calendar year + 1 for months Feb–Dec; same calendar year for Jan.
    # FQ1=Feb-Apr, FQ2=May-Jul, FQ3=Aug-Oct, FQ4=Nov-Jan.  Format: "FY26-Q4"
    return (
        f"'FY' || RIGHT(TO_CHAR(YEAR({e}) + IFF(MONTH({e}) >= 2, 1, 0)), 2) || '-Q' || "
        f"CASE WHEN MONTH({e}) IN (2,3,4) THEN '1' "
        f"     WHEN MONTH({e}) IN (5,6,7) THEN '2' "
        f"     WHEN MONTH({e}) IN (8,9,10) THEN '3' "
        f"     ELSE '4' END"
    )
def _expand(cols):
    return (
        f"    SELECT {cols}, REGION, MONTH_KEY AS PERIOD_KEY FROM base\n"
        f"    UNION ALL\n"
        f"    SELECT {cols}, REGION, QUARTER_KEY AS PERIOD_KEY FROM base\n"
        f"    UNION ALL\n"
        f"    SELECT {cols}, 'Theater' AS REGION, MONTH_KEY AS PERIOD_KEY FROM base\n"
        f"    UNION ALL\n"
        f"    SELECT {cols}, 'Theater' AS REGION, QUARTER_KEY AS PERIOD_KEY FROM base"
    )
# Derive AMSExpansion rep→region from current pipeline snapshot (REPORTING, no row access policy).
_OPP_MAX_DS = "(SELECT MAX(DS) FROM SALES.REPORTING.CORE_OPPORTUNITY_POST_SPLIT)"

def _setsail_reps_cte():
    """AEs: distinct OPPORTUNITY_OWNER_NAME → region."""
    return f"""amsexp_reps AS (
    SELECT DISTINCT OPPORTUNITY_OWNER_NAME AS OWNER, REGION
    FROM SALES.REPORTING.CORE_OPPORTUNITY_POST_SPLIT
    WHERE THEATER='AMSExpansion' AND DS={_OPP_MAX_DS}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY OPPORTUNITY_OWNER_NAME
                               ORDER BY SALES_QUALIFIED_DATE DESC NULLS LAST) = 1
)"""

def _setsail_dms_cte():
    """DMs: distinct DM column → region."""
    return f"""amsexp_dms AS (
    SELECT DISTINCT DM AS OWNER, REGION
    FROM SALES.REPORTING.CORE_OPPORTUNITY_POST_SPLIT
    WHERE THEATER='AMSExpansion' AND DS={_OPP_MAX_DS} AND DM IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DM
                               ORDER BY SALES_QUALIFIED_DATE DESC NULLS LAST) = 1
)"""

def _setsail_ses_cte():
    """SEs: LEAD_SALES_ENGINEER_NAME from AMSExpansion pipeline snapshot → region."""
    return f"""amsexp_ses AS (
    SELECT DISTINCT LEAD_SALES_ENGINEER_NAME AS OWNER, REGION
    FROM SALES.REPORTING.CORE_OPPORTUNITY_POST_SPLIT
    WHERE THEATER='AMSExpansion' AND DS={_OPP_MAX_DS}
      AND LEAD_SALES_ENGINEER_NAME IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY LEAD_SALES_ENGINEER_NAME
                               ORDER BY SALES_QUALIFIED_DATE DESC NULLS LAST) = 1
)"""


# ── SQL functions ─────────────────────────────────────────────────────────────
def sql_score_tacv_won():
    return f"""
WITH base AS (
    SELECT TOTAL_ACV, REGION, {_mk("CLOSE_DATE")} AS MONTH_KEY, {_qk("CLOSE_DATE")} AS QUARTER_KEY
    FROM SALES.REPORTING.CORE_BOOKINGS_POST_SPLIT
    WHERE THEATER='AMSExpansion' AND CLOSE_DATE>={_LOOKBACK}
), expanded AS ({_expand("TOTAL_ACV")})
SELECT COALESCE(SUM(TOTAL_ACV),0) AS TACV_WON,
       COUNT(*) AS DEALS_WON, REGION, PERIOD_KEY
FROM expanded GROUP BY REGION, PERIOD_KEY"""

def _uc_params(t):
    # DIM_USE_CASE_MDM_CACHE: CREATED_DATE is DATE (no cast); IS_DEPLOYED replaces IS_WENT_LIVE
    return {"created":("CREATED_DATE",""), "won":("DECISION_DATE"," AND IS_WON=TRUE"),
            "golive":("GO_LIVE_DATE"," AND IS_DEPLOYED=TRUE")}[t]

def sql_score_uc(t):
    de, we = _uc_params(t)
    col = {"created":"UC_CREATED","won":"UC_WON","golive":"UC_GOLIVE"}[t]
    return f"""
WITH base AS (
    SELECT USE_CASE_EACV AS USE_CASE_ACV, REGION_NAME AS REGION,
           {_mk(de)} AS MONTH_KEY, {_qk(de)} AS QUARTER_KEY
    FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE
    WHERE THEATER_NAME='AMSExpansion'{we} AND {de}>={_LOOKBACK}
), expanded AS ({_expand("USE_CASE_ACV")})
SELECT COALESCE(SUM(USE_CASE_ACV),0) AS {col},
       COUNT(*) AS {col}_COUNT, REGION, PERIOD_KEY
FROM expanded GROUP BY REGION, PERIOD_KEY"""

def sql_top_deals():
    """Single query for all Top Deals tabs (TACV / TCV / Growth ACV). Python sorts per tab."""
    return f"""
WITH {_SE_ATTR_CTE},
base AS (
    SELECT o.AE, COALESCE(se.SALES_ENGINEER_NAME,'—') AS SE, o.ACCOUNT_NAME,
           o.TOTAL_ACV, o.NET_TCV, o.GROWTH_ACV, o.DM, o.REGION,
           {_mk("o.CLOSE_DATE")} AS MONTH_KEY, {_qk("o.CLOSE_DATE")} AS QUARTER_KEY
    FROM SALES.REPORTING.CORE_BOOKINGS_POST_SPLIT o
    LEFT JOIN se_attr se ON se.ACCOUNT_ID=o.ACCOUNT_ID
    WHERE o.THEATER='AMSExpansion' AND o.CLOSE_DATE>={_LOOKBACK}
), expanded AS ({_expand("AE, SE, ACCOUNT_NAME, TOTAL_ACV, NET_TCV, GROWTH_ACV, DM")})
SELECT AE, SE, ACCOUNT_NAME, TOTAL_ACV, NET_TCV, GROWTH_ACV, DM, REGION, PERIOD_KEY
FROM expanded"""

def sql_ae_tacv_won():
    return f"""
WITH {_SE_ATTR_CTE},
base AS (
    SELECT o.AE, COALESCE(se.SALES_ENGINEER_NAME,'—') AS SE, o.TOTAL_ACV, o.REGION,
           {_mk("o.CLOSE_DATE")} AS MONTH_KEY, {_qk("o.CLOSE_DATE")} AS QUARTER_KEY
    FROM SALES.REPORTING.CORE_BOOKINGS_POST_SPLIT o
    LEFT JOIN se_attr se ON se.ACCOUNT_ID=o.ACCOUNT_ID
    WHERE o.THEATER='AMSExpansion' AND o.CLOSE_DATE>={_LOOKBACK}
), expanded AS ({_expand("AE, SE, TOTAL_ACV")})
SELECT AE, MAX(SE) AS SE, SUM(TOTAL_ACV) AS TACV_WON, COUNT(*) AS DEAL_COUNT, REGION, PERIOD_KEY
FROM expanded GROUP BY AE, REGION, PERIOD_KEY ORDER BY TACV_WON DESC NULLS LAST"""

def sql_dm_tacv_won():
    return f"""
WITH base AS (
    SELECT DM, TOTAL_ACV, REGION,
           {_mk("CLOSE_DATE")} AS MONTH_KEY, {_qk("CLOSE_DATE")} AS QUARTER_KEY
    FROM SALES.REPORTING.CORE_BOOKINGS_POST_SPLIT
    WHERE THEATER='AMSExpansion' AND CLOSE_DATE>={_LOOKBACK}
), expanded AS ({_expand("DM, TOTAL_ACV")})
SELECT DM, SUM(TOTAL_ACV) AS TACV_WON, COUNT(*) AS DEAL_COUNT, REGION, PERIOD_KEY
FROM expanded GROUP BY DM, REGION, PERIOD_KEY ORDER BY TACV_WON DESC NULLS LAST"""

def sql_ae_tacv_created():
    return f"""
WITH {_SE_ATTR_CTE},
base AS (
    SELECT o.OPPORTUNITY_OWNER_NAME AS AE, COALESCE(se.SALES_ENGINEER_NAME,'—') AS SE,
           o.TOTAL_ACV, o.REGION,
           {_mk("o.SALES_QUALIFIED_DATE")} AS MONTH_KEY, {_qk("o.SALES_QUALIFIED_DATE")} AS QUARTER_KEY
    FROM SALES.REPORTING.CORE_OPPORTUNITY_POST_SPLIT o
    LEFT JOIN se_attr se ON se.ACCOUNT_ID=o.ACCOUNT_ID
    WHERE o.THEATER='AMSExpansion' AND o.DS={_OPP_MAX_DS} AND o.SALES_QUALIFIED_DATE>={_LOOKBACK}
), expanded AS ({_expand("AE, SE, TOTAL_ACV")})
SELECT AE, MAX(SE) AS SE, SUM(TOTAL_ACV) AS TACV_CREATED, COUNT(*) AS DEAL_COUNT, REGION, PERIOD_KEY
FROM expanded GROUP BY AE, REGION, PERIOD_KEY ORDER BY TACV_CREATED DESC NULLS LAST"""

def sql_dm_tacv_created():
    return f"""
WITH base AS (
    SELECT DM, TOTAL_ACV, REGION,
           {_mk("SALES_QUALIFIED_DATE")} AS MONTH_KEY, {_qk("SALES_QUALIFIED_DATE")} AS QUARTER_KEY
    FROM SALES.REPORTING.CORE_OPPORTUNITY_POST_SPLIT
    WHERE THEATER='AMSExpansion' AND DS={_OPP_MAX_DS} AND SALES_QUALIFIED_DATE>={_LOOKBACK}
), expanded AS ({_expand("DM, TOTAL_ACV")})
SELECT DM, SUM(TOTAL_ACV) AS TACV_CREATED, COUNT(*) AS DEAL_COUNT, REGION, PERIOD_KEY
FROM expanded GROUP BY DM, REGION, PERIOD_KEY ORDER BY TACV_CREATED DESC NULLS LAST"""

def sql_top_uc(t):
    de, we = _uc_params(t)
    pf = f"u.{de}"
    return f"""
WITH {_SE_ATTR_CTE},
base AS (
    SELECT u.OWNER_NAME AS AE, COALESCE(se.SALES_ENGINEER_NAME,'—') AS SE,
           u.ACCOUNT_NAME, u.USE_CASE_NAME, u.USE_CASE_EACV AS USE_CASE_ACV,
           u.ACCOUNT_DM AS DM, u.REGION_NAME AS REGION,
           {_mk(pf)} AS MONTH_KEY, {_qk(pf)} AS QUARTER_KEY
    FROM SNOWPUBLIC.STREAMLIT.DIM_USE_CASE_MDM_CACHE u
    LEFT JOIN se_attr se ON se.ACCOUNT_ID=u.ACCOUNT_ID
    WHERE u.THEATER_NAME='AMSExpansion'{we} AND {pf}>={_LOOKBACK}
), expanded AS ({_expand("AE, SE, ACCOUNT_NAME, USE_CASE_NAME, USE_CASE_ACV, DM")})
SELECT AE, SE, ACCOUNT_NAME, USE_CASE_NAME, USE_CASE_ACV, DM, REGION, PERIOD_KEY
FROM expanded ORDER BY USE_CASE_ACV DESC NULLS LAST"""

def sql_meetings_all():
    """Single query: all AMSExpansion meeting counts tagged with ROLE (AE/DM/SE)."""
    return f"""
WITH {_setsail_reps_cte()},
{_setsail_dms_cte()},
{_setsail_ses_cte()},
all_reps AS (
    SELECT OWNER, REGION, 'AE' AS ROLE FROM amsexp_reps
    UNION SELECT OWNER, REGION, 'DM' AS ROLE FROM amsexp_dms
    UNION SELECT OWNER, REGION, 'SE' AS ROLE FROM amsexp_ses
),
base AS (
    SELECT s.OWNER_NAME AS OWNER, r.ROLE, s.SS_MEETINGS_TOTAL AS MEETING_COUNT, r.REGION,
           {_mk("s.DATE_SK")} AS MONTH_KEY, {_qk("s.DATE_SK")} AS QUARTER_KEY
    FROM SALES.REPORTING.BOB_USER_ACTIVITY_DAILY s
    JOIN all_reps r ON r.OWNER = s.OWNER_NAME
    WHERE s.DATE_SK >= {_LOOKBACK}
), expanded AS ({_expand("OWNER, ROLE, MEETING_COUNT")})
SELECT OWNER, ROLE, SUM(MEETING_COUNT) AS MEETING_COUNT, REGION, PERIOD_KEY
FROM expanded GROUP BY OWNER, ROLE, REGION, PERIOD_KEY ORDER BY MEETING_COUNT DESC NULLS LAST"""

def sql_consumption():
    return f"""
WITH {_SE_ATTR_CTE},
acct_dim AS (
    SELECT ACCOUNT_ID AS SALESFORCE_ACCOUNT_ID, ACCOUNT_NAME, OPPORTUNITY_OWNER_NAME AS AE, DM, REGION
    FROM SALES.REPORTING.CORE_OPPORTUNITY_POST_SPLIT
    WHERE THEATER='AMSExpansion' AND DS={_OPP_MAX_DS}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SALESFORCE_ACCOUNT_ID
                               ORDER BY SALES_QUALIFIED_DATE DESC NULLS LAST) = 1
),
monthly_revenue AS (
    SELECT cr.SALESFORCE_ACCOUNT_ID, a.ACCOUNT_NAME, a.AE,
           COALESCE(se.SALES_ENGINEER_NAME, '—') AS SE,
           a.DM, a.REGION,
           DATE_TRUNC('month', cr.GENERAL_DATE) AS month_year,
           SUM(cr.TOTAL_REVENUE) AS monthly_revenue
    FROM SALES.REPORTING.CORE_PRODUCT_CATEGORY_CONSUMPTION cr
    JOIN acct_dim a ON a.SALESFORCE_ACCOUNT_ID = cr.SALESFORCE_ACCOUNT_ID
    LEFT JOIN se_attr se ON se.ACCOUNT_ID = cr.SALESFORCE_ACCOUNT_ID
    WHERE DATE_TRUNC('month', cr.GENERAL_DATE) >= {_LOOKBACK_CONSUMPTION}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
), with_lags AS (
    SELECT *, LAG(monthly_revenue) OVER (PARTITION BY salesforce_account_id ORDER BY month_year) AS prev_month_rev,
           LAG(monthly_revenue,12) OVER (PARTITION BY salesforce_account_id ORDER BY month_year) AS prev_year_rev
    FROM monthly_revenue
), metrics AS (
    SELECT ACCOUNT_NAME, AE, SE, DM, REGION, monthly_revenue AS MONTHLY_REVENUE,
           CASE WHEN prev_month_rev IS NULL OR prev_month_rev=0 THEN NULL
                ELSE ROUND((monthly_revenue-prev_month_rev)/prev_month_rev*100,1) END AS MOM_PCT,
           CASE WHEN prev_month_rev IS NULL OR prev_month_rev=0 THEN NULL
                ELSE monthly_revenue - prev_month_rev END AS MOM_DELTA,
           CASE WHEN prev_year_rev IS NULL OR prev_year_rev=0 THEN NULL
                ELSE ROUND((monthly_revenue-prev_year_rev)/prev_year_rev*100,1) END AS YOY_PCT,
           CASE WHEN prev_year_rev IS NULL OR prev_year_rev=0 THEN NULL
                ELSE monthly_revenue - prev_year_rev END AS YOY_DELTA,
           TO_CHAR(month_year,'YYYY-MM') AS PERIOD_KEY
    FROM with_lags
)
SELECT ACCOUNT_NAME, AE, SE, DM, REGION, MONTHLY_REVENUE, MOM_PCT, MOM_DELTA, YOY_PCT, YOY_DELTA, PERIOD_KEY FROM metrics
UNION ALL
SELECT ACCOUNT_NAME, AE, SE, DM, 'Theater' AS REGION, MONTHLY_REVENUE, MOM_PCT, MOM_DELTA, YOY_PCT, YOY_DELTA, PERIOD_KEY FROM metrics"""


# ── HTML rendering helpers ────────────────────────────────────────────────────
def _rank(i):
    cls = {1:"rank-1", 2:"rank-2", 3:"rank-3"}.get(i, "rank-n")
    return f'<span class="rank {cls}">{i}</span>'

def _fmt_acv(val):
    try: v = float(val)
    except: return str(val) if val else "$0"
    if pd.isna(v) or v == 0: return "$0"
    if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
    if v >= 1_000:     return f"${round(v/1000):.0f}K"
    return f"${v:.0f}"

def _bar(val, max_val, kind="total", max_px=80):
    if not max_val or max_val == 0: return ""
    w = max(3, int(float(val) / float(max_val) * max_px))
    return f'<td class="bar-cell"><div class="bar-wrapper"><div class="bar bar-{kind}" style="width:{w}px"></div></div></td>'

def _pct(val):
    try: v = float(val)
    except: return '<td class="right muted">—</td>'
    if pd.isna(v): return '<td class="right muted">—</td>'
    cls = "pct-pos" if v > 0 else ("pct-neg" if v < 0 else "pct-neu")
    sign = "+" if v > 0 else ""
    return f'<td class="right mono {cls}">{sign}{v:.1f}%</td>'

def _esc(val):
    """HTML-escape a value so special chars (& < > ") don't corrupt the HTML template."""
    return _html_lib.escape(str(val)) if val is not None else ""

def _pair(primary, secondary=None):
    sec = f'<div class="pair-secondary">{_esc(secondary)}</div>' if secondary and secondary != "—" else ""
    return f'<div class="pair-primary">{_esc(primary)}</div>{sec}'

def _section(title, badge=None, note=None, content=""):
    b = f'<span class="badge">{badge}</span>' if badge else ""
    n = f'<span class="snote">{note}</span>' if note else ""
    return f"""
<div class="lb-section">
  <div class="section-header"><h2>{title}</h2>{b}{n}</div>
  {content}
</div>"""

def _empty_row(cols):
    return f'<tr><td colspan="{cols}" class="empty-row">No data for this period / region.</td></tr>'

def _top_n_ties(df, col, n):
    """Return top-n rows, expanding to include all ties at the nth position.
    Never expands when the cutoff value is zero (avoids flooding the table)."""
    if df.empty or col not in df.columns or len(df) <= n:
        return df
    cutoff = float(df.iloc[n - 1][col])
    if cutoff <= 0:
        return df.head(n)
    return df[df[col].astype(float) >= cutoff]


# ── Table builders ────────────────────────────────────────────────────────────
def _top_deals_table(df, sort_col, label, ae_deals=None):
    """Top 5 deals table (TACV / TCV / Growth ACV).
    ae_deals: optional dict {AE name -> deal count} to add a Deals column."""
    ncols = 5 if ae_deals is not None else 4
    if df.empty:
        return f'<table class="lb-table"><tbody>{_empty_row(ncols)}</tbody></table>'
    # Exclude zero/null values for the sort column (e.g. $0 Growth ACV deals)
    df = df.copy()
    df[sort_col] = pd.to_numeric(df[sort_col], errors="coerce")
    df = df[df[sort_col].fillna(0) > 0].sort_values(sort_col, ascending=False, na_position="last").reset_index(drop=True)
    if df.empty:
        return f'<table class="lb-table"><tbody>{_empty_row(ncols)}</tbody></table>'
    rows = ""
    for i, row in enumerate(_top_n_ties(df, sort_col, 5).itertuples(), 1):
        amt = getattr(row, sort_col, None)
        deals_td = ""
        if ae_deals is not None:
            cnt = ae_deals.get(str(row.AE), "")
            deals_td = f'<td class="right">{cnt}</td>'
        rows += f"""<tr>
          <td>{_rank(i)}</td>
          <td>{_pair(row.AE, row.SE)}</td>
          <td>{_esc(row.ACCOUNT_NAME)}</td>
          <td class="right mono">{_fmt_acv(amt)}</td>
          {deals_td}
        </tr>"""
    deals_th = '<th class="right">Deals</th>' if ae_deals is not None else ""
    return f"""<table class="lb-table">
      <thead><tr>
        <th style="width:36px">#</th><th>AE / SE</th><th>Account</th>
        <th class="right">{label}</th>{deals_th}
      </tr></thead><tbody>{rows}</tbody></table>"""

def _ae_leaderboard_table(df, val_col, count_col, val_label, count_label, se_col=None):
    """AE leaderboard with rank, name/SE, value, count, progress bar."""
    if df.empty:
        return f'<table class="lb-table"><tbody>{_empty_row(5)}</tbody></table>'
    df = df.copy()
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
    df = df[df[val_col].fillna(0) > 0].reset_index(drop=True)
    if df.empty:
        return f'<table class="lb-table"><tbody>{_empty_row(5)}</tbody></table>'
    max_val = df[val_col].max()
    rows = ""
    for i, row in enumerate(_top_n_ties(df, val_col, 5).itertuples(), 1):
        val = getattr(row, val_col, 0) or 0
        cnt = getattr(row, count_col, "") if count_col else ""
        se  = getattr(row, se_col, None) if se_col else None
        name = row.AE if hasattr(row, 'AE') else row.DM
        rows += f"""<tr>
          <td>{_rank(i)}</td>
          <td>{_pair(name, se)}</td>
          <td class="right mono">{_fmt_acv(val)}</td>
          <td class="right">{int(cnt) if cnt else ""}</td>
          {_bar(val, max_val)}
        </tr>"""
    return f"""<table class="lb-table">
      <thead><tr>
        <th style="width:36px">#</th><th>Name</th>
        <th class="right">{val_label}</th>
        <th class="right">{count_label}</th>
        <th class="bar-cell"></th>
      </tr></thead><tbody>{rows}</tbody></table>"""

def _dm_leaderboard_table(df, val_col, count_col, val_label, count_label):
    """DM leaderboard (no SE column)."""
    if df.empty:
        return f'<table class="lb-table"><tbody>{_empty_row(5)}</tbody></table>'
    df = df.copy()
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
    df = df[df[val_col].fillna(0) > 0].reset_index(drop=True)
    if df.empty:
        return f'<table class="lb-table"><tbody>{_empty_row(5)}</tbody></table>'
    max_val = df[val_col].max()
    rows = ""
    for i, row in enumerate(_top_n_ties(df, val_col, 5).itertuples(), 1):
        val = getattr(row, val_col, 0) or 0
        cnt = getattr(row, count_col, "") if count_col else ""
        rows += f"""<tr>
          <td>{_rank(i)}</td>
          <td class="primary">{_esc(row.DM)}</td>
          <td class="right mono">{_fmt_acv(val)}</td>
          <td class="right">{int(cnt) if cnt else ""}</td>
          {_bar(val, max_val)}
        </tr>"""
    return f"""<table class="lb-table">
      <thead><tr>
        <th style="width:36px">#</th><th>DM</th>
        <th class="right">{val_label}</th>
        <th class="right">{count_label}</th>
        <th class="bar-cell"></th>
      </tr></thead><tbody>{rows}</tbody></table>"""

def _top_uc_table(df, mode="ae"):
    """Top use cases detail table.
    mode='ae': show AE / SE / Account / Use Case / ACV
    mode='dm': show DM / Account / Use Case / ACV"""
    if mode == "dm":
        ncols = 5
        if df.empty:
            return f'<table class="lb-table"><tbody>{_empty_row(ncols)}</tbody></table>'
        rows = ""
        for i, row in enumerate(_top_n_ties(df, "USE_CASE_ACV", 5).itertuples(), 1):
            dm = row.DM if hasattr(row, "DM") and row.DM and str(row.DM) != "nan" else "—"
            rows += f"""<tr>
              <td>{_rank(i)}</td>
              <td class="primary">{_esc(dm)}</td>
              <td>{_esc(row.ACCOUNT_NAME)}</td>
              <td class="muted small">{_esc(row.USE_CASE_NAME)}</td>
              <td class="right mono">{_fmt_acv(row.USE_CASE_ACV)}</td>
            </tr>"""
        return f"""<table class="lb-table">
          <thead><tr>
            <th style="width:36px">#</th><th>DM</th>
            <th>Account</th><th>Use Case</th><th class="right">ACV</th>
          </tr></thead><tbody>{rows}</tbody></table>"""
    # mode == "ae"
    if df.empty:
        return f'<table class="lb-table"><tbody>{_empty_row(6)}</tbody></table>'
    rows = ""
    for i, row in enumerate(_top_n_ties(df, "USE_CASE_ACV", 5).itertuples(), 1):
        rows += f"""<tr>
          <td>{_rank(i)}</td>
          <td class="primary">{_esc(row.AE)}</td>
          <td class="muted">{_esc(row.SE)}</td>
          <td>{_esc(row.ACCOUNT_NAME)}</td>
          <td class="muted small">{_esc(row.USE_CASE_NAME)}</td>
          <td class="right mono">{_fmt_acv(row.USE_CASE_ACV)}</td>
        </tr>"""
    return f"""<table class="lb-table">
      <thead><tr>
        <th style="width:36px">#</th><th>AE</th><th>SE</th>
        <th>Account</th><th>Use Case</th><th class="right">ACV</th>
      </tr></thead><tbody>{rows}</tbody></table>"""

def _meetings_table(df):
    if df.empty:
        return f'<table class="lb-table"><tbody>{_empty_row(4)}</tbody></table>'
    max_val = df["MEETING_COUNT"].max() if "MEETING_COUNT" in df.columns else 1
    rows = ""
    for i, row in enumerate(_top_n_ties(df, "MEETING_COUNT", 5).itertuples(), 1):
        cnt = row.MEETING_COUNT or 0
        rows += f"""<tr>
          <td>{_rank(i)}</td>
          <td class="primary">{_esc(row.OWNER)}</td>
          <td class="right">{int(cnt)}</td>
          {_bar(cnt, max_val, kind="meeting")}
        </tr>"""
    return f"""<table class="lb-table">
      <thead><tr>
        <th style="width:36px">#</th><th>Name</th>
        <th class="right">Meetings</th><th class="bar-cell"></th>
      </tr></thead><tbody>{rows}</tbody></table>"""

def _consumption_table(df, sort_col, sort_label, is_pct=False, mode="ae"):
    """Top 5 consumption growth accounts sorted by sort_col (positive growth only).
    mode='ae': AE / SE / Account / Revenue / Growth
    mode='dm': DM / Account / Revenue / Growth"""
    ncols = 6 if mode == "ae" else 5
    if df.empty:
        return f'<table class="lb-table"><tbody>{_empty_row(ncols)}</tbody></table>'
    df = (df[df[sort_col].fillna(0) > 0]
          .sort_values(sort_col, ascending=False)
          .head(5)
          .reset_index(drop=True))
    if df.empty:
        return f'<table class="lb-table"><tbody>{_empty_row(ncols)}</tbody></table>'
    rows = ""
    for i, row in enumerate(df.itertuples(), 1):
        dm  = row.DM if hasattr(row, "DM") and row.DM and str(row.DM) != "nan" else "—"
        rev = row.MONTHLY_REVENUE or 0
        metric_val = getattr(row, sort_col, None)
        metric_td  = _pct(metric_val) if is_pct else f'<td class="right mono pct-pos">{_fmt_acv(metric_val)}</td>'
        if mode == "ae":
            se = getattr(row, "SE", None)
            rows += f"""<tr>
              <td>{_rank(i)}</td>
              <td>{_pair(row.AE, se)}</td>
              <td>{_esc(row.ACCOUNT_NAME)}</td>
              <td class="right mono">{_fmt_acv(rev)}</td>
              {metric_td}
            </tr>"""
        else:
            rows += f"""<tr>
              <td>{_rank(i)}</td>
              <td class="primary">{_esc(dm)}</td>
              <td>{_esc(row.ACCOUNT_NAME)}</td>
              <td class="right mono">{_fmt_acv(rev)}</td>
              {metric_td}
            </tr>"""
    if mode == "ae":
        header = ('<th style="width:36px">#</th><th>AE / SE</th>'
                  '<th>Account</th><th class="right">Revenue</th>'
                  f'<th class="right">{sort_label}</th>')
    else:
        header = ('<th style="width:36px">#</th><th>DM</th>'
                  '<th>Account</th><th class="right">Revenue</th>'
                  f'<th class="right">{sort_label}</th>')
    return f'<table class="lb-table"><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table>'


# ── Sidebar ───────────────────────────────────────────────────────────────────
PERIOD_OPTIONS  = _period_options()
DEFAULT_PERIOD  = _default_period()
MONTH_OPTIONS   = [p for p in PERIOD_OPTIONS if "FY" not in p]

with st.sidebar:
    st.markdown("## AMSExpansion\nPipeline Leaderboard")
    st.divider()
    region = st.selectbox("Region", _REGIONS, index=0)
    st.divider()

    def_months = [DEFAULT_PERIOD] if DEFAULT_PERIOD in MONTH_OPTIONS else [MONTH_OPTIONS[0]]
    periods = st.multiselect("Month", MONTH_OPTIONS, default=def_months, key="sel_months")
    if not periods:
        periods = def_months

    st.divider()
    lbl = ", ".join(periods) if len(periods) <= 2 else f"{len(periods)} months"
    st.caption(f"**{lbl}**")
    if st.button("Refresh Data"):
        _clear_cache(); st.rerun()

if region not in _REGIONS or not periods:
    st.error("Invalid selection."); st.stop()

# ── Parallel preload — all queries fire simultaneously on first load ──────────
_PRELOAD_SQLS = [
    sql_score_tacv_won(),
    sql_score_uc("created"), sql_score_uc("won"), sql_score_uc("golive"),
    sql_top_deals(),
    sql_ae_tacv_won(),    sql_dm_tacv_won(),
    sql_ae_tacv_created(), sql_dm_tacv_created(),
    sql_top_uc("created"), sql_top_uc("won"), sql_top_uc("golive"),
    sql_meetings_all(),
    sql_consumption(),
]
if not all(sql in _QUERY_CACHE for sql in _PRELOAD_SQLS):
    with st.spinner("Loading dashboard…"):
        _preload_parallel(_PRELOAD_SQLS)

# ── TEMPORARY DIAGNOSTICS — remove after debugging ───────────────────────────
with st.expander("🔍 Debug", expanded=True):
    st.write(f"**periods** = `{periods!r}`  **region** = `{region!r}`")
    st.write(f"**cache keys**: {len(_QUERY_CACHE)}")
    _sql_t = sql_score_tacv_won()
    if _sql_t in _QUERY_CACHE:
        _df_t, _ts_t = _QUERY_CACHE[_sql_t]
        st.write(f"**TACV cache**: {len(_df_t)} rows, age={_time.monotonic()-_ts_t:.0f}s")
        if not _df_t.empty:
            st.write(f"PERIOD_KEY sample: `{sorted(_df_t['PERIOD_KEY'].unique())[:6]}`")
            st.write(f"REGION sample: `{sorted(_df_t['REGION'].unique())[:6]}`")
    else:
        st.write("**TACV**: NOT IN CACHE")
    # Check session context
    try:
        from snowflake.snowpark.context import get_active_session as _gas
        _sess = _gas()
        _ctx = _sess.sql("SELECT CURRENT_USER() AS U, CURRENT_ROLE() AS R").collect()
        st.write(f"Session: user=`{_ctx[0]['U']}` role=`{_ctx[0]['R']}`")
    except Exception as _e:
        st.write(f"Session ERROR: `{_e}`")
    # Direct test: count rows in the view
    try:
        _df_d = _execute_sql(
            "SELECT COUNT(*) AS N FROM SALES.RAVEN.SDA_CLOSED_OPPORTUNITY_BOOKINGS_VIEW"
            " WHERE THEATER='AMSExpansion'"
        )
        st.write(f"RAVEN bookings: `{_df_d.iloc[0,0]}`")
    except Exception as _e:
        st.write(f"RAVEN bookings ERROR: `{_e}`")
    try:
        _df_r = _execute_sql(
            "SELECT COUNT(*) AS N FROM SALES.REPORTING.CORE_PRODUCT_CATEGORY_CONSUMPTION"
            " WHERE SALESFORCE_ACCOUNT_ID IS NOT NULL"
        )
        st.write(f"REPORTING consumption: `{_df_r.iloc[0,0]}`")
    except Exception as _e:
        st.write(f"REPORTING consumption ERROR: `{_e}`")
    try:
        _df_o = _execute_sql(
            "SELECT COUNT(*) AS N FROM SALES.RAVEN.SDA_OPPORTUNITY_VIEW"
            " WHERE THEATER='AMSExpansion'"
        )
        st.write(f"RAVEN opp view: `{_df_o.iloc[0,0]}`")
    except Exception as _e:
        st.write(f"RAVEN opp view ERROR: `{_e}`")
# ── END DIAGNOSTICS ───────────────────────────────────────────────────────────

# ── Header ────────────────────────────────────────────────────────────────────
region_label = "All Regions (Theater)" if region == "Theater" else region
st.markdown(f"""
<div class="lb-header">
  <h1>AMSExpansion Pipeline Leaderboard</h1>
  <div class="subtitle">{region_label} &nbsp;|&nbsp; {lbl}</div>
</div>""", unsafe_allow_html=True)

# ── Load scorecard data (all hits the module cache — no Snowflake round-trips) ─
df_sw    = _query(sql_score_tacv_won(),    region, periods)
df_uc_cr = _query(sql_score_uc("created"), region, periods)
df_uc_wo = _query(sql_score_uc("won"),     region, periods)
df_uc_gl = _query(sql_score_uc("golive"),  region, periods)
# Derive total AE meeting count from the unified meetings query (no extra DB call)
_df_mtg_score = _query(sql_meetings_all(), region, periods)
_total_meetings = int(
    _df_mtg_score.loc[_df_mtg_score["ROLE"] == "AE", "MEETING_COUNT"].sum()
) if not _df_mtg_score.empty else 0

def _s(df, vcol, ccol=None):
    if df.empty or vcol not in df.columns: return "—", "—"
    raw = pd.to_numeric(df[vcol], errors="coerce").sum()
    v = _fmt_acv(raw if raw is not None else 0)
    c = f"{int(pd.to_numeric(df[ccol], errors='coerce').sum() or 0):,}" if ccol and ccol in df.columns else ""
    return v, c

sw_v, sw_c   = _s(df_sw,    "TACV_WON",    "DEALS_WON")
cr_v, cr_c   = _s(df_uc_cr, "UC_CREATED",  "UC_CREATED_COUNT")
wo_v, wo_c   = _s(df_uc_wo, "UC_WON",      "UC_WON_COUNT")
gl_v, gl_c   = _s(df_uc_gl, "UC_GOLIVE",   "UC_GOLIVE_COUNT")
mtg_v = _total_meetings

# ── Summary cards ─────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="summary-grid">
  <div class="scard"><div class="val">{sw_v}</div><div class="sub">{sw_c} deals</div><div class="lbl">TACV Won</div></div>
  <div class="scard"><div class="val">{cr_v}</div><div class="sub">{cr_c} use cases</div><div class="lbl">UCs Created</div></div>
  <div class="scard"><div class="val">{wo_v}</div><div class="sub">{wo_c} use cases</div><div class="lbl">UCs Won</div></div>
  <div class="scard"><div class="val">{gl_v}</div><div class="sub">{gl_c} use cases</div><div class="lbl">UCs Go-Live</div></div>
  <div class="scard"><div class="val">{mtg_v:,}</div><div class="lbl">Total Meetings</div></div>
</div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TACV GROUP
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="group-label">TACV — Pipeline &amp; Bookings</div>', unsafe_allow_html=True)

df_top_deals  = _query(sql_top_deals(),         region, periods)
df_ae_won     = _query(sql_ae_tacv_won(),        region, periods)
if not df_ae_won.empty:
    df_ae_won = (df_ae_won.groupby("AE", as_index=False)
                 .agg({"SE": "first", "TACV_WON": "sum", "DEAL_COUNT": "sum"})
                 .sort_values("TACV_WON", ascending=False).reset_index(drop=True))
df_dm_won     = _query(sql_dm_tacv_won(),        region, periods)
if not df_dm_won.empty:
    df_dm_won = (df_dm_won.groupby("DM", as_index=False)
                 .agg({"TACV_WON": "sum", "DEAL_COUNT": "sum"})
                 .sort_values("TACV_WON", ascending=False).reset_index(drop=True))
df_ae_created = _query(sql_ae_tacv_created(),    region, periods)
if not df_ae_created.empty:
    df_ae_created = (df_ae_created.groupby("AE", as_index=False)
                     .agg({"SE": "first", "TACV_CREATED": "sum", "DEAL_COUNT": "sum"})
                     .sort_values("TACV_CREATED", ascending=False).reset_index(drop=True))
df_dm_created = _query(sql_dm_tacv_created(),    region, periods)
if not df_dm_created.empty:
    df_dm_created = (df_dm_created.groupby("DM", as_index=False)
                     .agg({"TACV_CREATED": "sum", "DEAL_COUNT": "sum"})
                     .sort_values("TACV_CREATED", ascending=False).reset_index(drop=True))

# Top Deals + TACV leaderboard — one unified section
st.markdown('<div class="lb-section"><div class="section-header"><h2>Top Deals</h2>'
            '<span class="badge">Top 5</span>'
            '<span class="snote">Closed Won in period</span></div></div>',
            unsafe_allow_html=True)
# Build AE deal-count lookup for the TACV tab's Deals column
ae_deal_counts = ({str(r.AE): int(r.DEAL_COUNT) for r in df_ae_won.itertuples()}
                  if not df_ae_won.empty else {})
tacv_view = st.radio("View by", ["Top AEs", "Top DMs"], horizontal=True, key="tacv_view",
                     label_visibility="collapsed")
t1, t2, t3, t4, t5 = st.tabs(["TACV", "TCV", "Growth ACV", "Won", "Created"])
with t1: st.html(_top_deals_table(df_top_deals, "TOTAL_ACV",  "TACV",       ae_deals=ae_deal_counts))
with t2: st.html(_top_deals_table(df_top_deals, "NET_TCV",    "TCV"))
with t3: st.html(_top_deals_table(df_top_deals, "GROWTH_ACV", "Growth ACV"))
with t4:
    if tacv_view == "Top AEs":
        st.html(_ae_leaderboard_table(df_ae_won, "TACV_WON", "DEAL_COUNT",
                                      "TACV Won", "Deals", se_col="SE"))
    else:
        st.html(_dm_leaderboard_table(df_dm_won, "TACV_WON", "DEAL_COUNT",
                                      "TACV Won", "Deals"))
with t5:
    if tacv_view == "Top AEs":
        st.html(_ae_leaderboard_table(df_ae_created, "TACV_CREATED", "DEAL_COUNT",
                                      "TACV Created", "Deals", se_col="SE"))
    else:
        st.html(_dm_leaderboard_table(df_dm_created, "TACV_CREATED", "DEAL_COUNT",
                                      "TACV Created", "Deals"))

# ══════════════════════════════════════════════════════════════════════════════
# USE CASES GROUP
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="group-label">Use Cases</div>', unsafe_allow_html=True)

df_uc_top = {t: _query(sql_top_uc(t), region, periods) for t in ("created","won","golive")}

# Top UCs — toggle + single set of tabs
st.markdown('<div class="lb-section"><div class="section-header"><h2>Top Use Cases</h2>'
            '<span class="badge">Top 5</span>'
            '<span class="snote">By ACV in period</span></div></div>',
            unsafe_allow_html=True)
uc_view = st.radio("View by", ["Top AEs", "Top DMs"], horizontal=True, key="uc_view",
                   label_visibility="collapsed")
u1, u2, u3 = st.tabs(["Created", "Won", "Go-Live"])
uc_mode = "ae" if uc_view == "Top AEs" else "dm"
with u1: st.html(_top_uc_table(df_uc_top["created"], mode=uc_mode))
with u2: st.html(_top_uc_table(df_uc_top["won"],     mode=uc_mode))
with u3: st.html(_top_uc_table(df_uc_top["golive"],  mode=uc_mode))


# ══════════════════════════════════════════════════════════════════════════════
# MEETINGS
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="group-label">Meetings</div>', unsafe_allow_html=True)

df_mtg_all = _query(sql_meetings_all(), region, periods)

# Aggregate across all selected months before splitting by role
def _agg_mtg(df, roles):
    if df.empty: return df
    return (df[df["ROLE"].isin(roles)]
            .groupby(["OWNER", "REGION"], as_index=False)["MEETING_COUNT"].sum()
            .sort_values("MEETING_COUNT", ascending=False).reset_index(drop=True))

df_mtg_ae  = _agg_mtg(df_mtg_all, ["AE"])
df_mtg_dm  = _agg_mtg(df_mtg_all, ["DM"])
df_mtg_ase = _agg_mtg(df_mtg_all, ["AE", "SE"])

mc1, mc2, mc3 = st.columns(3)
with mc1:
    st.html(_section("Top AEs",       content=_meetings_table(df_mtg_ae)))
with mc2:
    st.html(_section("Top DMs",       content=_meetings_table(df_mtg_dm)))
with mc3:
    st.html(_section("Top AEs & SEs", content=_meetings_table(df_mtg_ase)))

# ══════════════════════════════════════════════════════════════════════════════
# CONSUMPTION
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="group-label">Consumption</div>', unsafe_allow_html=True)

df_cons = _query(sql_consumption(), region, [periods[0]])
st.markdown('<div class="lb-section"><div class="section-header"><h2>Consumption Growth</h2>'
            '<span class="badge">Top 5</span>'
            '<span class="snote">Positive growth only</span></div></div>',
            unsafe_allow_html=True)
cons_view = st.radio("View by", ["Top AEs", "Top DMs"], horizontal=True, key="cons_view",
                     label_visibility="collapsed")
cons_mode = "ae" if cons_view == "Top AEs" else "dm"
c1, c2, c3, c4 = st.tabs(["MoM %", "MoM $", "YoY %", "YoY $"])
with c1: st.html(_consumption_table(df_cons, "MOM_PCT",   "MoM Growth %",  is_pct=True,  mode=cons_mode))
with c2: st.html(_consumption_table(df_cons, "MOM_DELTA", "MoM Growth $",  is_pct=False, mode=cons_mode))
with c3: st.html(_consumption_table(df_cons, "YOY_PCT",   "YoY Growth %",  is_pct=True,  mode=cons_mode))
with c4: st.html(_consumption_table(df_cons, "YOY_DELTA", "YoY Growth $",  is_pct=False, mode=cons_mode))

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="text-align:center;padding:20px;font-size:11px;color:#64748b">
  AMSExpansion Pipeline Leaderboard &nbsp;·&nbsp; {region_label} &nbsp;·&nbsp; {lbl}
</div>""", unsafe_allow_html=True)
