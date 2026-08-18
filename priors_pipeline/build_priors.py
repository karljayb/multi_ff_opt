"""
build_priors.py — weekly pipeline to produce pl_player_priors.csv + pl_team_priors.csv

Data sources:
  1. Existing priors CSV (carry-forward baseline)      — Supabase input-data
  2. FPL API                                           — plain HTTP
  3. FotMob Method A (per-player stats)                — plain HTTP
  4. FotMob Method B (team stats)                      — plain HTTP
  5a. FBref match logs (p60_gstart, p90_gstart)        — SeleniumBase UC
  5b. Transfermarkt injury/suspension history          — SeleniumBase UC
  6. Merge + derived column computation                — pandas
  7. Write output CSVs back to Supabase input-data     — Supabase storage

Call run_build_priors(job_id, ...) from the API layer.
"""

import gzip
import io
import json
import re
import time
import urllib.parse
from datetime import date as _date, timedelta as _timedelta

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from core import storage
from core.log import job_log, log_failure, get_failures
from core.jobs import is_cancelled

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FPL_BOOTSTRAP    = "https://fantasy.premierleague.com/api/bootstrap-static/"
FOTMOB_LEAGUE_ID = 47
FOTMOB_SEASON_ID = 36781   # 2026/2027 PL
FOTMOB_STAT_BASE = f"https://data.fotmob.com/stats/{FOTMOB_LEAGUE_ID}/season/{FOTMOB_SEASON_ID}"
FOTMOB_NEXT_BASE = "https://www.fotmob.com/_next/data"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

FOTMOB_DELAY  = 2.5
FBREF_DELAY   = 2.5
FBREF_SEASON  = "2026-2027"
FBREF_PRIOR_SEASON = "2025-2026"  # fallback when FBREF_SEASON has no PL rows yet (new-to-PL signings, pre-season)

TM_BASE  = "https://www.transfermarkt.co.uk"
TM_DELAY = 3.0

# Penalty conversion constants
PEN_CONV       = 0.79
LEAGUE_PENS_PG = 0.14
K_PEN          = PEN_CONV * LEAGUE_PENS_PG   # 0.1106

# Supabase paths
BUCKET          = "input-data"
PLAYER_PRIORS   = "pl_player_priors.csv"
TEAM_PRIORS     = "pl_team_priors.csv"
ABSENCE_HISTORY = "player_absence_history.csv"
TM_ID_MAP       = "tm_id_map.csv"

# ---------------------------------------------------------------------------
# FotMob stat key → (priors_col, is_per90)
# ---------------------------------------------------------------------------
PLAYER_STAT_KEYS = {
    "goals":                        ("g",             True),
    "non_penalty_xg":               ("npxg",          True),
    "ShotsOnTarget":                ("sot",           True),
    "expected_goals_on_target":     ("_xgot",         True),
    "expected_assists":             ("xa",            True),
    "big_chance_created_team_title":("bcc",           True),
    "successful_passes":            ("acc_pass",      True),
    "crosses_succeeeded":           ("s_cross",       True),
    "dribbles_succeeded":           ("s_dribb",       True),
    "matchstats.headers.tackles":   ("tack_att",      True),
    "interceptions":                ("int",           True),
    "blocked_shots":                ("block",         True),
    "clearances":                   ("clear",         True),
    "recoveries":                   ("recov",         True),
    "fouls_won":                    ("foul_w",        True),
    "fouls":                        ("foul_l",        True),
    "yellow_cards":                 ("yc",            True),
    "red_cards":                    ("s_rc",          True),
    "error_led_to_goal":            ("eltg",          True),
    "saves":                        ("_gk_saves_p90", True),
    "goals_conceded":               ("_gk_ga_p90",    True),
    "goals_prevented":              ("_gk_gprev",     False),
}

# Columns from PLAYER_STAT_KEYS where "more = better production" — safe to discount
# down when the underlying data came from a weaker league via the fbref fallback
# (see _discount_by_source_league). Deliberately excludes negative/ambiguous-valence
# columns (yc, s_rc, foul_l, eltg, _gk_ga_p90, _gk_gprev) — discounting "goals conceded"
# or "errors leading to a goal" the same direction as attacking output would be backwards
# (a weak-league keeper's low concession rate should, if anything, look worse against
# PL attacks, not better), and there's no verified basis here for the right magnitude
# of an inverted discount, so those are left undiscounted rather than guessed at.
OUTPUT_RATE_DISCOUNT_COLS = [
    "g", "npxg", "sot", "_xgot", "xa", "bcc", "acc_pass", "s_cross", "s_dribb",
    "tack_att", "int", "block", "clear", "recov", "foul_w", "_gk_saves_p90",
]

# Placeholder ratios (Premier League = 1.0) — a domain judgment call, not derived from
# code. Overridable via settings/meta_settings.json["source_league_strength"]; see
# GET/PUT /api/priors/source-league-strength.
SOURCE_LEAGUE_STRENGTH_DEFAULT = {
    "Championship": 0.65,
    "Bundesliga":   0.90,
    "Serie A":      0.90,
    "La Liga":      0.92,
    "Ligue 1":      0.80,
    "Eredivisie":   0.65,
    "Süper Lig":    0.55,
    "_default":     0.60,
}

TEAM_STAT_ENDPOINTS = {
    "goals_team_match":              ("g_f",       True),
    "goals_conceded_team_match":     ("g_a",       True),
    "expected_goals_team":           ("_xgf",      False),
    "expected_goals_conceded_team":  ("_xga",      False),
    "ontarget_scoring_att_team":     ("sot_f",     True),
    "accurate_pass_team":            ("acc_pass_f",True),
    "accurate_cross_team":           ("s_cross_f", True),
    "total_tackle_team":             ("tack_w_f",  True),
    "interception_team":             ("int_f",     True),
    "effective_clearance_team":      ("clear_f",   True),
    "big_chance_team":               ("bcc_f",     True),
    "penalty_won_team":              ("pen_f",     False),
    "penalty_conceded_team":         ("pen_a",     False),
    "fk_foul_lost_team":             ("foul_l_f",  True),
    "total_yel_card_team":           ("yc_f",      True),
    "total_red_card_team":           ("s_rc_f",    True),
}


# ===========================================================================
# STEP 1  —  Load existing priors from Supabase
# ===========================================================================

def load_existing_priors() -> tuple[pd.DataFrame, pd.DataFrame]:
    job_log("Loading existing priors from Supabase …")
    players = pd.read_csv(io.BytesIO(storage.download(BUCKET, PLAYER_PRIORS)))
    teams   = pd.read_csv(io.BytesIO(storage.download(BUCKET, TEAM_PRIORS)))
    players["fotmob_id"]    = pd.to_numeric(players["fotmob_id"],    errors="coerce").astype("Int64")
    teams["fotmob_team_id"] = pd.to_numeric(teams["fotmob_team_id"], errors="coerce").astype("Int64")
    job_log(f"  {len(players)} players, {len(teams)} teams loaded")
    return players, teams


# ===========================================================================
# STEP 2  —  FPL API
# ===========================================================================

def fetch_fpl_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    job_log("Fetching FPL bootstrap …")
    r = requests.get(FPL_BOOTSTRAP, headers={"User-Agent": BROWSER_HEADERS["User-Agent"]}, timeout=30)
    r.raise_for_status()
    data = r.json()

    fpl_players = pd.DataFrame(data["elements"])[
        ["code", "web_name", "element_type", "team", "status", "news"]
    ].rename(columns={
        "code":         "fpl_code",
        "element_type": "fpl_element_type",
        "team":         "fpl_team_id",
    })
    fpl_players["is_injured"]      = fpl_players["status"].isin(["i", "u"])
    fpl_players["expected_return"] = fpl_players["news"].where(
        fpl_players["status"].isin(["i", "u", "d"]), other=""
    )
    fpl_players.drop(columns=["status", "news"], inplace=True)

    fpl_teams = pd.DataFrame(data["teams"])[["id", "code", "name", "short_name"]].rename(columns={
        "id":         "fpl_team_id",
        "code":       "fpl_team_code",
        "name":       "fpl_team_name",
        "short_name": "fpl_short_team_name",
    })

    job_log(f"  FPL: {len(fpl_players)} players, {len(fpl_teams)} teams")
    return fpl_players, fpl_teams


# ===========================================================================
# STEP 3  —  FotMob Method A (per-player stats)
# ===========================================================================

_PAGE_HEADERS = {"User-Agent": BROWSER_HEADERS["User-Agent"]}


def get_fotmob_build_id() -> str:
    job_log("Fetching FotMob buildId …")
    r = requests.get(
        f"https://www.fotmob.com/leagues/{FOTMOB_LEAGUE_ID}",
        headers=_PAGE_HEADERS,
        timeout=30,
    )
    r.raise_for_status()
    match = re.search(r'"buildId"\s*:\s*"([^"]+)"', r.text)
    if not match:
        raise RuntimeError("FotMob buildId not found in page source")
    build_id = match.group(1)
    job_log(f"  buildId = {build_id}")
    return build_id


def _parse_player_page(data: dict) -> dict:
    result: dict = {}
    try:
        season = data["pageProps"]["data"]["firstSeasonStats"]
    except (KeyError, TypeError):
        return result
    if season is None:
        raise ValueError("no season stats available (firstSeasonStats is null)")

    for item in season.get("topStatCard", {}).get("items", []):
        if item.get("localizedTitleId") == "minutes_played":
            try:
                result["sample_mins"] = float(item["statValue"])
            except (ValueError, TypeError):
                pass
            break

    for section in season.get("statsSection", {}).get("items", []):
        for item in section.get("items", []):
            key = item.get("localizedTitleId", "")
            if key not in PLAYER_STAT_KEYS:
                continue
            col, is_per90 = PLAYER_STAT_KEYS[key]
            try:
                val = float(item["per90"] if is_per90 else item["statValue"])
            except (ValueError, TypeError):
                continue
            result[col] = val

    return result


def fetch_fotmob_player_stats(
    players_df: pd.DataFrame,
    build_id: str,
    limit: int | None = None,
    _job_id: str | None = None,
) -> pd.DataFrame:
    valid = players_df[players_df["fotmob_id"].notna()].copy()
    if limit is not None:
        valid = valid.head(limit)
    total = len(valid)
    est_min = int(total * FOTMOB_DELAY // 60)
    job_log(f"FotMob Method A: {total} players (~{est_min} min) …")

    records = []
    for i, (_, row) in enumerate(valid.iterrows()):
        fid = int(row["fotmob_id"])
        url = f"{FOTMOB_NEXT_BASE}/{build_id}/players/{fid}.json"
        try:
            r = requests.get(url, headers=BROWSER_HEADERS, timeout=20)
            if r.status_code == 404:
                job_log("  buildId stale (404) — refreshing …")
                build_id = get_fotmob_build_id()
                url = f"{FOTMOB_NEXT_BASE}/{build_id}/players/{fid}.json"
                r = requests.get(url, headers=BROWSER_HEADERS, timeout=20)
            r.raise_for_status()
            stats = _parse_player_page(r.json())
            stats["fotmob_id"] = fid
            records.append(stats)
        except Exception as e:
            log_failure("fotmob", f"id={fid}", str(e))
            records.append({"fotmob_id": fid})

        if (i + 1) % 50 == 0:
            job_log(f"  … {i+1}/{total} FotMob players done")
        if _job_id and is_cancelled(_job_id):
            job_log("  Cancelled — stopping FotMob scrape")
            break
        time.sleep(FOTMOB_DELAY)

    df = pd.DataFrame(records)
    df["fotmob_id"] = df["fotmob_id"].astype("Int64")
    return df


# ===========================================================================
# STEP 4  —  FotMob Method B (team stats)
# ===========================================================================

def _fetch_fotmob_team_endpoint(stat_name: str) -> list[dict]:
    url = f"{FOTMOB_STAT_BASE}/{stat_name}.json"
    r = requests.get(url, headers=BROWSER_HEADERS, timeout=30)
    r.raise_for_status()
    try:
        payload = json.loads(gzip.decompress(r.content))
    except Exception:
        payload = r.json()
    return payload["TopLists"][0]["StatList"]


def fetch_fotmob_team_stats() -> pd.DataFrame:
    job_log("FotMob team stats (Method B) …")
    records: dict[int, dict] = {}

    for stat_name, (col, is_per_game) in TEAM_STAT_ENDPOINTS.items():
        job_log(f"  → {stat_name}")
        try:
            stat_list = _fetch_fotmob_team_endpoint(stat_name)
        except Exception as e:
            job_log(f"    FAILED ({e}), skipping")
            continue

        for item in stat_list:
            tid     = item.get("TeamId")
            val     = item.get("StatValue")
            matches = item.get("MatchesPlayed", 1) or 1
            if tid is None or val is None:
                continue
            tid = int(tid)
            records.setdefault(tid, {})["fotmob_team_id"] = tid
            records[tid][col] = val if is_per_game else val / matches

        time.sleep(0.5)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(list(records.values()))
    df["fotmob_team_id"] = df["fotmob_team_id"].astype("Int64")
    return df


# ===========================================================================
# STEP 5a  —  FBref match logs via SeleniumBase UC
# ===========================================================================

FBREF_MATCHLOG_URL = (
    "https://fbref.com/en/players/{fbref_code}/matchlogs/{season}/misc/{slug}-Match-Logs"
)


def _fbref_slug(name: str) -> str:
    return re.sub(r"[^\w\s-]", "", name).strip().replace(" ", "-")


def _int_from_cell(cell) -> int:
    if cell is None:
        return 0
    txt = cell.get_text(strip=True)
    try:
        return int(txt) if txt else 0
    except ValueError:
        return 0


def _parse_matchlog_html(html: str, comp_filter: str | None = "Premier League") -> dict:
    """
    Parse an fbref "Miscellaneous Stats" match-log table into start-rate stats
    plus per-90 offsides/own-goals rates (both otherwise unavailable anywhere in
    the pipeline — see Documentation/"Priors Columns EV Usage Audit.md").

    comp_filter="Premier League" (default): only count rows in that competition —
      used for the current-season scrape.
    comp_filter=None: don't filter by competition. Instead, find whichever
      competition has the most rows in the table (a domestic league season always
      has far more matches than any cup/Europe run — verified against real fbref
      output for three different players) and compute start rates from just that
      competition's rows. Used for the prior-season fallback, where we don't know
      in advance which league the player was in. Returns a "source_league" key
      naming the competition that was used.
    """
    html = re.sub(r"<!--(.*?)-->", r"\1", html, flags=re.DOTALL)
    soup  = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "matchlogs_all"})
    if table is None:
        return {}

    rows = []
    for row in table.find("tbody").find_all("tr"):
        classes = row.get("class", [])
        if "thead" in classes or "partial_table" in classes:
            continue

        comp_cell = row.find(["td", "th"], {"data-stat": "comp"})
        comp = comp_cell.get_text(strip=True) if comp_cell else None
        if not comp:
            continue

        start_cell   = row.find("td", {"data-stat": "game_started"})
        mins_cell    = row.find("td", {"data-stat": "minutes"})
        offside_cell = row.find("td", {"data-stat": "offsides"})
        og_cell      = row.find("td", {"data-stat": "own_goals"})

        try:
            mins = float(mins_cell.get_text(strip=True) or 0) if mins_cell else 0.0
        except ValueError:
            mins = 0.0

        started = bool(start_cell and start_cell.get_text(strip=True).startswith("Y"))
        offsides = _int_from_cell(offside_cell)
        own_goals = _int_from_cell(og_cell)
        rows.append((comp, started, mins, offsides, own_goals))

    if comp_filter is not None:
        target_comp = comp_filter
    else:
        comp_counts: dict[str, int] = {}
        for comp, *_ in rows:
            comp_counts[comp] = comp_counts.get(comp, 0) + 1
        if not comp_counts:
            return {}
        target_comp = max(comp_counts, key=comp_counts.get)

    starts_count    = 0
    starts_60_count = 0
    starts_90_count = 0
    total_mins      = 0.0
    total_offsides  = 0
    total_og        = 0
    for comp, started, mins, offsides, own_goals in rows:
        if comp != target_comp:
            continue
        total_mins     += mins
        total_offsides += offsides
        total_og       += own_goals
        if not started:
            continue
        starts_count += 1
        if mins >= 60:
            starts_60_count += 1
        if mins >= 90:
            starts_90_count += 1

    if starts_count == 0:
        return {}
    result = {
        "p60_gstart": round(starts_60_count / starts_count, 6),
        "p90_gstart": round(starts_90_count / starts_count, 6),
    }
    if total_mins > 0:
        result["offside"] = round(total_offsides / total_mins * 90, 6)
        result["og"]      = round(total_og / total_mins * 90, 6)
    if comp_filter is None:
        result["source_league"] = target_comp
    return result


def _is_dead_session(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "invalid session id" in msg or "connection refused" in msg or "disconnected" in msg


def _load_fbref_page(driver, url: str) -> str:
    """Open an fbref URL and return page HTML, trying a GUI captcha click once if Cloudflare is still showing."""
    driver.uc_open_with_reconnect(url, reconnect_time=8)
    time.sleep(3)
    html = driver.page_source
    if "matchlogs_all" not in html:
        try:
            driver.uc_gui_click_captcha()
            time.sleep(4)
            html = driver.page_source
        except Exception as ce:
            job_log(f"  uc_gui_click_captcha failed: {ce}")
    return html


def _scrape_player_matchlogs(driver, fbref_code: str, ref_name: str, skip_primary: bool = False) -> tuple[dict, str | None]:
    """
    Scrape one player's fbref match logs for FBREF_SEASON. If that has no
    Premier League rows (new-to-PL signing, or the season hasn't started yet),
    fall back to FBREF_PRIOR_SEASON and use whichever competition the player
    mostly played there — recorded as "source_league" in the returned dict.
    Returns (summary_dict, primary_season_html) — the html is exposed only so
    the caller can still save it for Cloudflare debugging (None when the
    primary request was skipped).

    skip_primary=True bypasses the current-season request entirely and goes
    straight to FBREF_PRIOR_SEASON — used while the PL season hasn't started
    yet, since the primary request is then guaranteed empty for every player
    and doing it anyway would roughly double fbref page-load volume for no
    benefit (this is what triggered a full-night Cloudflare block on the run
    that first shipped the fallback, when literally every candidate hit both
    requests back to back).
    """
    slug = _fbref_slug(ref_name)
    html = None
    if not skip_primary:
        url = FBREF_MATCHLOG_URL.format(fbref_code=fbref_code, season=FBREF_SEASON, slug=slug)
        html = _load_fbref_page(driver, url)
        summary = _parse_matchlog_html(html, comp_filter="Premier League")
        if summary.get("p60_gstart") is not None:
            summary["source_league"] = "Premier League"
            return summary, html
        # Pace the fallback request instead of firing it immediately — avoids a
        # 2x-speed burst against fbref's Cloudflare for every player that needs it.
        time.sleep(FBREF_DELAY)

    fallback_url = FBREF_MATCHLOG_URL.format(fbref_code=fbref_code, season=FBREF_PRIOR_SEASON, slug=slug)
    fallback_html = _load_fbref_page(driver, fallback_url)
    fallback_summary = _parse_matchlog_html(fallback_html, comp_filter=None)
    return fallback_summary, (html if html is not None else fallback_html)


def fetch_fbref_match_logs(
    players_df: pd.DataFrame,
    driver,
    limit: int | None = None,
    _job_id: str | None = None,
) -> tuple[pd.DataFrame, object]:
    """
    Scrape FBref match logs using a SeleniumBase UC driver.
    Returns (DataFrame, driver) — driver may have been restarted mid-run.
    """
    # Filter to non-unavailable FPL players with an fbref identity — not restricted
    # to players with prior PL minutes, since new-to-PL signings (0 PL minutes) are
    # exactly who needs the prior-season fallback below.
    r = requests.get(FPL_BOOTSTRAP, headers={"User-Agent": BROWSER_HEADERS["User-Agent"]}, timeout=30)
    r.raise_for_status()
    boot = r.json()
    active_fpl_codes = {
        str(p["code"]) for p in boot["elements"]
        if p.get("status") != "u"
    }
    # While the season hasn't started, FBREF_SEASON is guaranteed empty for every
    # player — skip it entirely and go straight to the fallback season, instead of
    # doubling fbref page-load volume for a guaranteed-empty request on every
    # candidate (see _scrape_player_matchlogs docstring).
    season_started = any(e.get("is_current") or e.get("finished") for e in boot["events"])
    if not season_started:
        job_log(f"  {FBREF_SEASON} hasn't started yet — skipping it, going straight to {FBREF_PRIOR_SEASON}")

    has_code = players_df[
        players_df["fbref_code"].notna()
        & (players_df["fbref_code"].astype(str) != "")
        & (players_df["fpl_code"].astype(str).isin(active_fpl_codes))
    ].copy()

    if limit is not None:
        has_code = has_code.head(limit)

    total = len(has_code)
    job_log(f"FBref match logs: {total} players to scrape …")

    records = []
    for i, (_, row) in enumerate(has_code.iterrows()):
        fid = int(row["fotmob_id"]) if pd.notna(row["fotmob_id"]) else None
        rec = {"fotmob_id": fid}
        try:
            summary, html = _scrape_player_matchlogs(driver, row["fbref_code"], str(row["ref_name"]), skip_primary=not season_started)
            # Debug: save first player's HTML to Supabase so we can inspect what CF returns
            if i == 0:
                try:
                    storage.upload(BUCKET, "fbref_debug.html", html.encode("utf-8", errors="replace"), "text/html")
                    job_log("  DEBUG: first player HTML saved to Supabase fbref_debug.html")
                except Exception:
                    pass
            rec.update({"p60_gstart": summary.get("p60_gstart"), "p90_gstart": summary.get("p90_gstart"),
                        "p60_gstart_source_league": summary.get("source_league"),
                        "og": summary.get("og"), "offside": summary.get("offside")})
            p60 = summary.get("p60_gstart")
            source_league = summary.get("source_league")
            if p60 is not None and source_league == "Premier League":
                job_log(f"  [{i+1}/{total}] {row['ref_name']}: p60={p60:.3f}")
            elif p60 is not None:
                job_log(f"  [{i+1}/{total}] {row['ref_name']}: p60={p60:.3f} (fallback: {source_league}, {FBREF_PRIOR_SEASON})")
            else:
                log_failure("fbref", row['ref_name'], "no matchlog data")
        except Exception as e:
            if _is_dead_session(e):
                job_log(f"  FBref browser session died — restarting ({row['ref_name']})")
                try:
                    driver.quit()
                except Exception:
                    pass
                try:
                    driver = _make_driver()
                    job_log("  Browser restarted — warming up CF clearance …")
                    try:
                        driver.uc_open_with_reconnect("https://fbref.com/", reconnect_time=8)
                        time.sleep(4)
                        if "fbref" not in driver.page_source.lower():
                            driver.uc_gui_click_captcha()
                            time.sleep(4)
                        job_log("  CF warm-up done — retrying player")
                    except Exception as wu:
                        job_log(f"  CF warm-up warning: {wu}")
                    summary, _ = _scrape_player_matchlogs(driver, row["fbref_code"], str(row["ref_name"]), skip_primary=not season_started)
                    rec.update({"p60_gstart": summary.get("p60_gstart"), "p90_gstart": summary.get("p90_gstart"),
                                "p60_gstart_source_league": summary.get("source_league"),
                                "og": summary.get("og"), "offside": summary.get("offside")})
                    p60 = summary.get("p60_gstart")
                    source_league = summary.get("source_league")
                    if p60 is not None and source_league == "Premier League":
                        job_log(f"  [{i+1}/{total}] {row['ref_name']} (retry): p60={p60:.3f}")
                    elif p60 is not None:
                        job_log(f"  [{i+1}/{total}] {row['ref_name']} (retry): p60={p60:.3f} (fallback: {source_league}, {FBREF_PRIOR_SEASON})")
                    else:
                        log_failure("fbref", row['ref_name'], "no matchlog data (retry)")
                except Exception as e2:
                    log_failure("fbref", row['ref_name'], f"retry failed: {e2}")
            else:
                job_log(f"  FBref {row['ref_name']}: {e}")
        records.append(rec)

        if (i + 1) % 20 == 0:
            job_log(f"  … {i+1}/{total} FBref done")
        if _job_id and is_cancelled(_job_id):
            job_log("  Cancelled — stopping FBref scrape")
            break
        time.sleep(FBREF_DELAY)

    df = pd.DataFrame(records)
    df["fotmob_id"] = df["fotmob_id"].astype("Int64")
    if len(df) > 0 and "og" in df.columns and df["p60_gstart"].notna().any() and df["og"].isna().all():
        job_log(
            "  WARNING: p60_gstart populated but og/offside came back empty for every "
            "player — fbref's misc-page data-stat names may have changed, check fbref_debug.html"
        )
    return df, driver


# ===========================================================================
# STEP 5b  —  Transfermarkt injury/suspension history via SeleniumBase UC
# ===========================================================================

def _load_tm_id_map() -> pd.DataFrame:
    try:
        raw = storage.download(BUCKET, TM_ID_MAP)
        df  = pd.read_csv(io.BytesIO(raw))
    except Exception:
        return pd.DataFrame(columns=["fotmob_id", "tm_id", "last_scraped"])
    df["fotmob_id"]    = pd.to_numeric(df["fotmob_id"],    errors="coerce").astype("Int64")
    df["tm_id"]        = pd.to_numeric(df["tm_id"],        errors="coerce").astype("Int64")
    df["last_scraped"] = pd.to_datetime(df["last_scraped"], errors="coerce")
    return df


def _save_tm_id_map(df: pd.DataFrame) -> None:
    out = df.copy()
    out["last_scraped"] = (
        out["last_scraped"].dt.strftime("%Y-%m-%d").where(out["last_scraped"].notna(), other="")
    )
    storage.upload(BUCKET, TM_ID_MAP, out.to_csv(index=False).encode(), "text/csv")


def _parse_tm_date(text: str) -> str | None:
    from datetime import datetime
    text = text.strip()
    if not text or text in ("-", "?", "present", "unknown", "today"):
        return None
    for fmt in ("%b %d, %Y", "%d/%m/%Y", "%Y-%m-%d", "%d.%m.%Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def _parse_tm_table(html: str, absence_type: str, season_start: str = "2026-07-01") -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    records = []
    table = soup.find("table", {"class": "items"})
    if table is None:
        return records
    tbody = table.find("tbody")
    if tbody is None:
        return records
    for row in tbody.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
        dates = [_parse_tm_date(c) for c in cells if _parse_tm_date(c)]
        if not dates:
            continue
        from_date = dates[0]
        to_date   = dates[1] if len(dates) > 1 else _date.today().isoformat()
        if to_date < from_date:
            to_date = from_date
        if to_date < season_start:
            continue
        from_date = max(from_date, season_start)
        records.append({"from_date": from_date, "to_date": to_date, "absence_type": absence_type})
    return records


def _expand_absence_ranges(fotmob_id: int, ranges: list[dict]) -> list[dict]:
    records = []
    for r in ranges:
        try:
            d   = _date.fromisoformat(r["from_date"])
            end = _date.fromisoformat(r["to_date"])
            while d <= end:
                records.append({
                    "fotmob_id":    fotmob_id,
                    "date":         d.isoformat(),
                    "absence_type": r["absence_type"],
                })
                d += _timedelta(days=1)
        except (ValueError, KeyError):
            continue
    return records


def _normalize_name(name: str) -> str:
    import unicodedata
    return unicodedata.normalize("NFD", name).encode("ascii", "ignore").decode().lower()


def _name_variants(name: str) -> list[str]:
    parts = name.strip().split()
    variants: list[str] = [name]
    if len(parts) > 2:
        variants.append(f"{parts[0]} {parts[-1]}")
    if len(parts) > 1:
        variants.append(parts[0])
    seen: set[str] = set()
    out: list[str] = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _search_tm_player(driver, name: str, team_name: str) -> int | None:
    if not name:
        return None
    team_word = team_name.lower().split()[0] if team_name else ""

    for variant in _name_variants(name):
        query = urllib.parse.quote(variant)
        url = (
            f"{TM_BASE}/schnellsuche/ergebnis/schnellsuche"
            f"?query={query}&Scheinfeld=Spieler&Ber=spieler"
        )
        try:
            driver.get(url)
            time.sleep(1)
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")

            for a_tag in soup.find_all("a", href=re.compile(r"/spieler/\d+")):
                m = re.search(r"/spieler/(\d+)", a_tag["href"])
                if not m:
                    continue
                parent = a_tag.find_parent("tr") or a_tag.find_parent("td")
                if parent and team_word and team_word in parent.get_text(" ", strip=True).lower():
                    return int(m.group(1))

            first = soup.find("a", href=re.compile(r"/spieler/\d+"))
            if first:
                m = re.search(r"/spieler/(\d+)", first["href"])
                if m:
                    return int(m.group(1))

        except Exception as e:
            job_log(f"  TM search failed for '{variant}': {e}")

        time.sleep(TM_DELAY)

    return None


def fetch_transfermarkt_injuries(
    players_df: pd.DataFrame,
    driver,
    limit: int | None = None,
    _job_id: str | None = None,
    force_all: bool = False,
) -> pd.DataFrame:
    """
    Scrape Transfermarkt injury/suspension history using an existing SeleniumBase driver.
    Uses tm_id_map.csv (stored in Supabase) for player ID crosswalk.
    Incremental: skips players scraped within the last 7 days, unless:
      - force_all=True (scrapes everyone regardless of last_scraped), or
      - the player is currently doubtful/injured in FPL (always re-scraped).
    """
    tm_map = _load_tm_id_map()
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)

    fid_to_tmid: dict[int, int] = {
        int(r["fotmob_id"]): int(r["tm_id"])
        for _, r in tm_map.iterrows()
        if pd.notna(r["fotmob_id"]) and pd.notna(r["tm_id"])
    }
    fid_to_last: dict[int, pd.Timestamp] = {
        int(r["fotmob_id"]): r["last_scraped"]
        for _, r in tm_map.iterrows()
        if pd.notna(r["fotmob_id"]) and pd.notna(r["last_scraped"])
    }

    r = requests.get(FPL_BOOTSTRAP, headers={"User-Agent": BROWSER_HEADERS["User-Agent"]}, timeout=30)
    r.raise_for_status()
    fpl_elements = r.json()["elements"]
    active_fpl_codes = {
        str(p["code"]) for p in fpl_elements
        if p.get("status") != "u" and p.get("minutes", 0) > 0
    }
    # Players currently doubtful or injured in FPL — always re-scrape regardless of cooldown
    doubtful_fpl_codes = {
        str(p["code"]) for p in fpl_elements
        if p.get("status") in ("d", "i")
    }

    active = players_df[
        players_df["fotmob_id"].notna()
        & players_df["fpl_code"].astype(str).isin(active_fpl_codes)
    ].copy()
    if limit is not None:
        active = active.head(limit)

    # fpl_code → currently available ("a") for recovery detection
    available_fpl_codes = {
        str(p["code"]) for p in fpl_elements
        if p.get("status") == "a"
    }

    if force_all:
        job_log("  force_all=True: scraping all players (ignoring last_scraped)")
    else:
        before = len(active)
        def _needs_scrape(row) -> bool:
            fid      = int(row["fotmob_id"])
            fpl_code = str(row.get("fpl_code", ""))
            # Re-scrape if status changed: newly doubtful/injured
            if fpl_code in doubtful_fpl_codes:
                return True
            # Re-scrape if status changed: was injured/unavailable in priors, now available
            if row.get("is_injured") and fpl_code in available_fpl_codes:
                return True
            last = fid_to_last.get(fid)
            if last is None or pd.isna(last):
                return True
            return last < cutoff

        mask = active.apply(_needs_scrape, axis=1)
        skipped = before - mask.sum()
        active = active[mask]
        if skipped:
            job_log(f"  Incremental: skipping {skipped} players (scraped within 7 days, no status change)")

    total = len(active)
    job_log(f"Transfermarkt injury/suspension scrape: {total} players …")

    absence_records: list[dict] = []
    updated_map_rows: list[dict] = []

    for i, (_, row) in enumerate(active.iterrows()):
        fid   = int(row["fotmob_id"])
        tm_id = fid_to_tmid.get(fid)
        name  = str(row.get("fotmob_name", ""))
        team  = str(row.get("current_team", ""))

        if tm_id is None:
            tm_id = _search_tm_player(driver, name, team)
            if tm_id:
                fid_to_tmid[fid] = tm_id
                job_log(f"  Found TM ID {tm_id} for {name}")
            else:
                log_failure("tm", name, "no TM ID found")
                continue
            time.sleep(TM_DELAY)

        player_ranges: list[dict] = []

        try:
            driver.get(f"{TM_BASE}/player/verletzungen/spieler/{tm_id}")
            time.sleep(1)
            player_ranges.extend(_parse_tm_table(driver.page_source, "injured"))
        except Exception as e:
            log_failure("tm", name, f"injury page failed: {e}")
        time.sleep(TM_DELAY)

        try:
            driver.get(f"{TM_BASE}/player/sperren/spieler/{tm_id}")
            time.sleep(1)
            player_ranges.extend(_parse_tm_table(driver.page_source, "suspended"))
        except Exception as e:
            log_failure("tm", name, f"suspension page failed: {e}")
        time.sleep(TM_DELAY)

        absence_records.extend(_expand_absence_ranges(fid, player_ranges))
        updated_map_rows.append({
            "fotmob_id":    fid,
            "tm_id":        tm_id,
            "last_scraped": pd.Timestamp.today().normalize(),
        })

        if (i + 1) % 20 == 0:
            job_log(f"  … {i+1}/{total} TM done")
        if _job_id and is_cancelled(_job_id):
            job_log("  Cancelled — stopping Transfermarkt scrape")
            break

    # Upsert tm_id_map and save back to Supabase
    if updated_map_rows:
        updated_df = pd.DataFrame(updated_map_rows)
        updated_df["fotmob_id"] = updated_df["fotmob_id"].astype("Int64")
        updated_df["tm_id"]     = updated_df["tm_id"].astype("Int64")
        upserted_fids = set(updated_df["fotmob_id"].dropna())
        tm_map = tm_map[~tm_map["fotmob_id"].isin(upserted_fids)]
        # Drop all-NA rows before concat to avoid FutureWarning
        tm_map = tm_map.dropna(how="all")
        tm_map = pd.concat([tm_map, updated_df], ignore_index=True)
        tm_map = tm_map.sort_values("fotmob_id").reset_index(drop=True)
        _save_tm_id_map(tm_map)
        job_log(f"  Updated tm_id_map.csv ({len(updated_map_rows)} players)")

    # Merge with existing absence history (keep records for players not re-scraped)
    existing: pd.DataFrame | None = None
    try:
        raw      = storage.download(BUCKET, ABSENCE_HISTORY)
        existing = pd.read_csv(io.BytesIO(raw))
    except Exception:
        pass

    if not absence_records:
        return existing if existing is not None else pd.DataFrame(
            columns=["fotmob_id", "date", "absence_type"]
        )

    new_df = pd.DataFrame(absence_records)
    new_df["fotmob_id"] = new_df["fotmob_id"].astype("Int64")

    if existing is not None:
        rescraped_fids = new_df["fotmob_id"].dropna().unique()
        existing = existing[~existing["fotmob_id"].isin(rescraped_fids)]
        new_df = pd.concat([existing, new_df], ignore_index=True)

    new_df = (
        new_df
        .drop_duplicates(subset=["fotmob_id", "date"])
        .sort_values(["fotmob_id", "date"])
        .reset_index(drop=True)
    )
    return new_df


# ===========================================================================
# STEP 6  —  Merge + compute derived columns
# ===========================================================================

def _merge_onto(base: pd.DataFrame, new: pd.DataFrame, key: str) -> pd.DataFrame:
    if new.empty:
        return base
    new = new.copy()
    new[key] = new[key].astype(base[key].dtype)
    merged = base.merge(new, on=key, how="left", suffixes=("", "_new"))
    for col in new.columns:
        if col == key:
            continue
        new_col = f"{col}_new"
        if new_col in merged.columns:
            merged[col] = merged[new_col].combine_first(merged[col]) if col in merged.columns else merged[new_col]
            merged.drop(columns=[new_col], inplace=True)
    return merged


def _load_source_league_strength() -> dict:
    try:
        overrides = storage.download_json("settings", "meta_settings.json").get("source_league_strength", {})
    except Exception:
        overrides = {}
    return {**SOURCE_LEAGUE_STRENGTH_DEFAULT, **overrides}


def _discount_by_source_league(fotmob: pd.DataFrame, fbref: pd.DataFrame) -> pd.DataFrame:
    """
    Discount this run's freshly-scraped fotmob output-rate stats for players whose
    fbref fallback pass (see _scrape_player_matchlogs) found their most recent data
    came from a non-PL league. Applied once here, to the raw fresh fotmob pull,
    before it's merged into the carried-forward priors — never to already-merged/
    carried-forward columns — so repeated nightly runs can't compound the discount.
    Players fbref didn't scrape this run (empty/missing "p60_gstart_source_league")
    are left untouched.
    """
    if fbref.empty or "p60_gstart_source_league" not in fbref.columns or fotmob.empty:
        return fotmob

    league_by_id = fbref.set_index("fotmob_id")["p60_gstart_source_league"]
    fotmob = fotmob.copy()
    league = fotmob["fotmob_id"].map(league_by_id)
    needs_discount = league.notna() & (league != "Premier League")
    if not needs_discount.any():
        return fotmob

    ratios = _load_source_league_strength()
    ratio = league[needs_discount].map(lambda lg: ratios.get(lg, ratios["_default"]))
    cols = [c for c in OUTPUT_RATE_DISCOUNT_COLS if c in fotmob.columns]
    for col in cols:
        fotmob.loc[needs_discount, col] = fotmob.loc[needs_discount, col] * ratio
    return fotmob


def compute_player_priors(
    existing:    pd.DataFrame,
    fotmob:      pd.DataFrame,
    fbref:       pd.DataFrame,
    fpl_players: pd.DataFrame,
) -> pd.DataFrame:
    df = existing.copy()

    fotmob = _discount_by_source_league(fotmob, fbref)
    df = _merge_onto(df, fotmob, "fotmob_id")
    fbref_cols = ["fotmob_id"] + [c for c in ["p60_gstart", "p90_gstart", "p60_gstart_source_league", "og", "offside"] if c in fbref.columns]
    df = _merge_onto(df, fbref[fbref_cols], "fotmob_id")

    fpl_players["fpl_code"] = pd.to_numeric(fpl_players["fpl_code"], errors="coerce").astype("Int64")
    df["fpl_code"]          = pd.to_numeric(df["fpl_code"],           errors="coerce").astype("Int64")
    df = _merge_onto(df, fpl_players[["fpl_code", "is_injured", "expected_return"]], "fpl_code")

    # GK sv_p_xgot
    if {"_gk_saves_p90", "_gk_ga_p90", "_gk_gprev", "sample_mins"}.issubset(df.columns):
        mins        = df["sample_mins"].replace(0, np.nan)
        ga_total    = df["_gk_ga_p90"]    * mins / 90.0
        saves_total = df["_gk_saves_p90"] * mins / 90.0
        psxg        = ga_total + df["_gk_gprev"]
        sv_new      = saves_total / psxg.replace(0, np.nan)
        gk_mask = df["primary_position"] == "GK"
        if "sv_p_xgot" not in df.columns:
            df["sv_p_xgot"] = np.nan
        df.loc[gk_mask & psxg.notna() & (psxg > 0), "sv_p_xgot"] = sv_new[gk_mask & psxg.notna() & (psxg > 0)]
        df.loc[gk_mask & df["sv_p_xgot"].isna(), "sv_p_xgot"] = 2.15

    # fin_skill / elev_g
    if {"npxg", "g", "on_pens"}.issubset(df.columns):
        # on_pens has no automated source (carry-forward only) — players never
        # manually assigned a value stay NaN forever, which poisons fin_skill
        # below and, downstream, the whole team's goalscorer weighting in
        # ev/calculator.py's np_scorers_from_fix (a single NaN teammate zeroes
        # every teammate's EV for that fixture). Default to 0 (not a pen taker).
        df["on_pens"] = df["on_pens"].fillna(0)
        df["elev_g"]  = df["npxg"] + df["on_pens"] * K_PEN
        safe_npxg     = df["npxg"].replace(0, np.nan)
        fin_skill_new = (df["g"] - K_PEN * df["on_pens"]) / safe_npxg
        df["fin_skill"] = fin_skill_new.where(fin_skill_new > 0, other=df["fin_skill"])

    df.drop(columns=[c for c in df.columns if c.startswith("_")], inplace=True, errors="ignore")

    # ev/calculator.py reads player_dict["p60_gstart"] as a required (non-.get) key for
    # every in-pool player — surface anyone current-FPL and still NaN here (e.g. genuinely
    # no senior football history anywhere, like an academy-only player) instead of letting
    # it silently reach that required-key access.
    if "p60_gstart" in df.columns:
        still_missing = df[df["fpl_code"].isin(fpl_players["fpl_code"]) & df["p60_gstart"].isna()]
        if not still_missing.empty:
            names = ", ".join(still_missing["ref_name"].head(10).astype(str))
            more = " ..." if len(still_missing) > 10 else ""
            job_log(f"  WARNING: {len(still_missing)} active FPL players still have no p60_gstart "
                     f"after fbref + prior-season fallback: {names}{more}")

    return df


def refresh_team_affiliation(
    df:             pd.DataFrame,
    fpl_players:    pd.DataFrame,
    fpl_teams:      pd.DataFrame,
    existing_teams: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Correct current_team/current_team_id from this run's FPL bootstrap data, which already
    reflects transfers and loan returns (FPL's own "team" field always points at one of the
    20 current PL clubs a player is registered to for FPL purposes). Bridges FPL's own team-id
    space to the fotmob_team_id space current_team_id uses via fpl_team_code, a stable per-club
    code present in both this run's fpl_teams and the carried-forward existing_teams.

    Players previously seeded (non-null fpl_code) who no longer appear in this run's FPL
    bootstrap at all (contract expired, left the league — e.g. Salah/Sancho summer 2026) have
    their team-identity fields nulled out rather than dropped, so their stat history survives
    but they fall out of the active pool via the current_team_id-not-in-team_data filter that
    ev/generate.py and data/players.py already use. parent_team_id must be nulled alongside
    current_team_id, or that same filter's fallback silently re-admits them.
    """
    crosswalk = fpl_teams[["fpl_team_id", "fpl_team_code"]].merge(
        existing_teams[["fotmob_team_id", "fotmob_team_name", "fpl_team_code"]],
        on="fpl_team_code", how="left",
    )
    fpl_code_team = fpl_players[["fpl_code", "fpl_team_id"]].merge(
        crosswalk[["fpl_team_id", "fotmob_team_id", "fotmob_team_name"]],
        on="fpl_team_id", how="left",
    ).drop(columns=["fpl_team_id"])
    # Plain float64 (not nullable Int64) so the equality check below uses ordinary NaN
    # semantics — a nullable-Int64-vs-float comparison can produce pd.NA, which .loc[]
    # boolean indexing rejects outright.
    fpl_code_team["fotmob_team_id"] = pd.to_numeric(fpl_code_team["fotmob_team_id"], errors="coerce")

    df = df.copy()
    fpl_code_team["fpl_code"] = pd.to_numeric(fpl_code_team["fpl_code"], errors="coerce").astype("Int64")
    df["fpl_code"] = pd.to_numeric(df["fpl_code"], errors="coerce").astype("Int64")
    df["current_team_id"] = pd.to_numeric(df["current_team_id"], errors="coerce")
    merged = df.merge(fpl_code_team, on="fpl_code", how="left", suffixes=("", "_new"))

    had_fpl_code   = df["fpl_code"].notna()
    resolved       = merged["fotmob_team_id"].notna()
    still_in_fpl   = merged["fpl_code"].isin(fpl_players["fpl_code"])
    changed        = had_fpl_code & resolved & (merged["fotmob_team_id"] != df["current_team_id"])
    departed       = had_fpl_code & ~still_in_fpl

    log_lines = []
    if changed.any():
        samples = df.loc[changed, "ref_name"].head(10).tolist()
        more = " ..." if changed.sum() > 10 else ""
        log_lines.append(f"\nTeam affiliation corrected ({int(changed.sum())}): {', '.join(samples)}{more}")
        df.loc[changed, "current_team_id"] = merged.loc[changed, "fotmob_team_id"]
        df.loc[changed, "current_team"]    = merged.loc[changed, "fotmob_team_name"]

    if departed.any():
        samples = df.loc[departed, "ref_name"].head(10).tolist()
        more = " ..." if departed.sum() > 10 else ""
        log_lines.append(f"\nPlayers no longer in FPL — excluded from active pool ({int(departed.sum())}): {', '.join(samples)}{more}")
        df.loc[departed, ["current_team", "current_team_id", "parent_team", "parent_team_id"]] = np.nan

    if not log_lines:
        log_lines.append("\nNo team affiliation changes this run.")

    return df, log_lines


def compute_team_priors(
    existing:  pd.DataFrame,
    team_raw:  pd.DataFrame,
    fpl_teams: pd.DataFrame,
) -> pd.DataFrame:
    df = existing.copy()
    df = _merge_onto(df, team_raw, "fotmob_team_id")

    if "_xgf" in df.columns and "pen_f" in df.columns:
        df["npxg_f"] = df["_xgf"] - df["pen_f"] * PEN_CONV
    if "_xga" in df.columns and "pen_a" in df.columns:
        df["npxg_a"] = df["_xga"] - df["pen_a"] * PEN_CONV

    if "g_f" in df.columns and "g_a" in df.columns:
        df["gd"] = df["g_f"] - df["g_a"]
    if "npxg_f" in df.columns and "pen_f" in df.columns:
        df["elev_g_f"] = df["npxg_f"] + df["pen_f"]
    if "npxg_a" in df.columns and "pen_a" in df.columns:
        df["elev_g_a"] = df["npxg_a"] + df["pen_a"]

    df.drop(columns=[c for c in df.columns if c.startswith("_")], inplace=True, errors="ignore")
    return df


# ===========================================================================
# STEP 7  —  Flag new players + write outputs to Supabase
# ===========================================================================

def flag_new_players(fpl_players: pd.DataFrame, fpl_teams: pd.DataFrame, existing: pd.DataFrame) -> list[str]:
    """
    Diffs against the FPL bootstrap (fpl_players), not FotMob — FotMob's per-player
    fetch only ever queries players who already have a fotmob_id in `existing`
    (see fetch_fotmob_player_stats), so its id set can never contain anyone new.
    fpl_players is independently fetched fresh every run and is the one source
    that actually includes players never yet seeded into the priors file.
    """
    existing_codes = set(existing["fpl_code"].dropna().astype(int))
    missing = fpl_players[~fpl_players["fpl_code"].astype(int).isin(existing_codes)]
    missing = missing.merge(
        fpl_teams[["fpl_team_id", "fpl_short_team_name"]], on="fpl_team_id", how="left"
    )
    lines = []
    if not missing.empty:
        lines.append(f"\nNEW PLAYERS — manual fotmob_id/fbref_code required ({len(missing)}):")
        for _, row in missing.sort_values("fpl_short_team_name").iterrows():
            lines.append(f"  {row['web_name']} ({row['fpl_short_team_name']}) — fpl_code={int(row['fpl_code'])}")
    else:
        lines.append("\nNo new players requiring manual ID assignment.")
    return lines


def write_outputs(
    player_df:   pd.DataFrame,
    team_df:     pd.DataFrame,
    player_cols: list[str],
    team_cols:   list[str],
    log_lines:   list[str],
    absence_df:  pd.DataFrame | None = None,
) -> None:
    storage.upload(
        BUCKET, PLAYER_PRIORS,
        player_df.reindex(columns=player_cols).to_csv(index=False).encode(), "text/csv",
    )
    storage.upload(
        BUCKET, TEAM_PRIORS,
        team_df.reindex(columns=team_cols).to_csv(index=False).encode(), "text/csv",
    )
    job_log(f"Written: {PLAYER_PRIORS}  ({len(player_df)} rows)")
    job_log(f"Written: {TEAM_PRIORS}  ({len(team_df)} rows)")

    if absence_df is not None and not absence_df.empty:
        storage.upload(
            BUCKET, ABSENCE_HISTORY,
            absence_df.to_csv(index=False).encode(), "text/csv",
        )
        job_log(f"Written: {ABSENCE_HISTORY}  ({len(absence_df)} records)")

    job_log("\nUpdate log:\n" + "\n".join(log_lines))


# ===========================================================================
# Run history (Supabase)
# ===========================================================================

CURRENT_RUN_FILE = "priors_current_run.json"
HISTORY_FILE     = "priors_run_history.json"
HISTORY_MAX      = 10


def _write_current_run(record: dict) -> None:
    """Overwrite priors_current_run.json with the latest state. Silently swallows errors."""
    try:
        storage.upload_json(BUCKET, CURRENT_RUN_FILE, record)
    except Exception:
        pass


def _finalise_history(record: dict) -> None:
    """Prepend the finished run to priors_run_history.json (rolling HISTORY_MAX)."""
    try:
        try:
            history = json.loads(storage.download(BUCKET, HISTORY_FILE))
        except Exception:
            history = []
        history.insert(0, record)
        history = history[:HISTORY_MAX]
        storage.upload(BUCKET, HISTORY_FILE, json.dumps(history).encode(), "application/json")
    except Exception:
        pass


# ===========================================================================
# Entry point
# ===========================================================================

def run_build_priors(
    job_id:       str,
    skip_fbref:   bool = False,
    skip_tm:      bool = False,
    absence_only: bool = False,
    force_all_tm: bool = False,
    limit:        int | None = None,
) -> None:
    """
    Main entry point called from the API layer as a background task.

    Args:
        job_id:       Background job ID (for SSE log streaming).
        skip_fbref:   Skip FBref scraping; p60_gstart/p90_gstart carry forward.
        skip_tm:      Skip Transfermarkt scraping; absence history unchanged.
        absence_only: Only run Transfermarkt and write player_absence_history.csv.
        force_all_tm: Bypass the 7-day cooldown and re-scrape all players on TM.
                      When absence_only=True this forces a full rescrape of everyone.
                      When False (default), doubtful/injured FPL players are always
                      re-scraped; fit players respect the 7-day cooldown.
        limit:        Cap scraping at N players (for testing).
    """
    from core.log import set_job_context
    from core.jobs import complete_job, fail_job

    set_job_context(job_id)

    started_at = pd.Timestamp.now().isoformat()
    _write_current_run({"job_id": job_id, "started_at": started_at, "status": "running", "logs": []})

    try:
        _run(skip_fbref=skip_fbref, skip_tm=skip_tm, absence_only=absence_only, force_all_tm=force_all_tm, limit=limit, job_id=job_id, started_at=started_at)
        finished_at = pd.Timestamp.now().isoformat()
        final_logs = []
        try:
            from core.jobs import get_job
            j = get_job(job_id)
            if j:
                final_logs = j.get("logs", [])
        except Exception:
            pass
        complete_job(job_id, {"status": "done"})
        record = {"job_id": job_id, "started_at": started_at, "finished_at": finished_at, "status": "complete", "logs": final_logs, "failures": get_failures()}
        _write_current_run(record)
        _finalise_history(record)
    except Exception as e:
        finished_at = pd.Timestamp.now().isoformat()
        final_logs = []
        try:
            from core.jobs import get_job
            j = get_job(job_id)
            if j:
                final_logs = j.get("logs", [])
        except Exception:
            pass
        fail_job(job_id, str(e))
        record = {"job_id": job_id, "started_at": started_at, "finished_at": finished_at, "status": "failed", "error": str(e), "logs": final_logs, "failures": get_failures()}
        _write_current_run(record)
        _finalise_history(record)
        raise


def _make_driver():
    """Create and return a SeleniumBase UC driver.

    On Linux, manually starts an Xvfb virtual display so that
    uc_gui_click_captcha() can solve interactive Cloudflare Turnstile challenges.
    Falls back to headless2 if Xvfb is unavailable.
    Prefers google-chrome-stable over chromium-browser.
    """
    import sys
    import shutil
    import subprocess
    import os
    import time as _time
    from seleniumbase import Driver

    kwargs = {"uc": True}

    if sys.platform == "linux":
        has_chrome = shutil.which("google-chrome-stable") or shutil.which("google-chrome")
        if not has_chrome:
            chrome_path = shutil.which("chromium-browser") or shutil.which("chromium")
            if chrome_path:
                kwargs["binary_location"] = chrome_path

        xvfb_bin = shutil.which("Xvfb")
        if xvfb_bin and not os.environ.get("DISPLAY"):
            try:
                subprocess.Popen(
                    [xvfb_bin, ":99", "-screen", "0", "1920x1080x24", "-ac"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                os.environ["DISPLAY"] = ":99"
                _time.sleep(1)
                job_log("  Xvfb virtual display started on :99")
            except Exception as xe:
                job_log(f"  WARNING: Xvfb start failed ({xe}) — falling back to headless2")
                kwargs["headless2"] = True
        elif not xvfb_bin:
            job_log("  WARNING: Xvfb not found — falling back to headless2")
            kwargs["headless2"] = True
        else:
            job_log(f"  DISPLAY already set ({os.environ['DISPLAY']}) — skipping Xvfb")

    else:
        kwargs["headless2"] = True

    driver = Driver(**kwargs)
    mode = "headless2" if kwargs.get("headless2") else f"xvfb (DISPLAY={os.environ.get('DISPLAY')})"
    job_log(f"  Browser started (mode: {mode})")
    return driver


def _run(
    skip_fbref:   bool = False,
    skip_tm:      bool = False,
    absence_only: bool = False,
    force_all_tm: bool = False,
    limit:        int | None = None,
    job_id:       str | None = None,
    started_at:   str | None = None,
) -> None:
    # Browser is created lazily just before it's needed (Steps 5a/5b),
    # not here — avoids Chromium crashing while idle during the long FotMob scrape.
    driver = None

    try:
        if absence_only:
            existing_players, _ = load_existing_priors()
            job_log("Starting SeleniumBase UC browser (headless) …")
            try:
                driver = _make_driver()
                job_log("  Browser ready")
            except Exception as e:
                job_log(f"  WARNING: browser failed to start ({e}) — absence-only run aborted")
                return
            if driver:
                absence_df = fetch_transfermarkt_injuries(existing_players, driver, limit=limit, _job_id=job_id, force_all=force_all_tm)
                if not absence_df.empty:
                    storage.upload(
                        BUCKET, ABSENCE_HISTORY,
                        absence_df.to_csv(index=False).encode(), "text/csv",
                    )
                    job_log(f"Written: {ABSENCE_HISTORY}  ({len(absence_df)} records)")
                else:
                    job_log("No absence records produced — file not written")
            else:
                job_log("No browser available — absence-only run aborted")
            return

        log_lines = [f"build_priors  {pd.Timestamp.now().isoformat()}"]

        # Step 1
        existing_players, existing_teams = load_existing_priors()
        player_cols = existing_players.columns.tolist()
        team_cols   = existing_teams.columns.tolist()
        # write_outputs() reindexes to exactly this list, so any new column has to be
        # added here explicitly the first time or it's silently dropped on every write.
        for _new_col in ("p60_gstart_source_league",):
            if _new_col not in player_cols:
                player_cols.append(_new_col)

        # Step 2
        fpl_players, fpl_teams = fetch_fpl_data()

        def _checkpoint(label: str) -> None:
            """Flush current logs to priors_current_run.json at each step boundary."""
            if not job_id:
                return
            try:
                from core.jobs import get_job
                j = get_job(job_id)
                logs = j.get("logs", []) if j else []
            except Exception:
                logs = []
            _write_current_run({"job_id": job_id, "started_at": started_at, "status": "running", "step": label, "logs": logs})

        # Step 3
        if job_id and is_cancelled(job_id):
            return
        build_id     = get_fotmob_build_id()
        fotmob_stats = fetch_fotmob_player_stats(existing_players, build_id, limit=limit, _job_id=job_id)
        _checkpoint("FotMob player stats done")

        # Step 4
        if job_id and is_cancelled(job_id):
            return
        team_raw = fetch_fotmob_team_stats()
        _checkpoint("FotMob team stats done")

        # Step 5a + 5b — start browser lazily here, after all HTTP steps are done
        if job_id and is_cancelled(job_id):
            return
        needs_browser = (not skip_fbref) or (not skip_tm)
        if needs_browser:
            job_log("Starting SeleniumBase UC browser (headless) …")
            try:
                driver = _make_driver()
                job_log("  Browser ready")
            except Exception as e:
                job_log(f"  WARNING: browser failed to start ({e}) — FBref/TM steps will be skipped")
                skip_fbref = True
                skip_tm    = True

        if not skip_fbref and driver:
            fbref_stats, driver = fetch_fbref_match_logs(existing_players, driver, limit=limit, _job_id=job_id)
        else:
            reason = "--skip-fbref" if skip_fbref else "no browser"
            job_log(f"Skipping FBref ({reason})")
            fbref_stats = pd.DataFrame(columns=["fotmob_id", "p60_gstart", "p90_gstart"])
            log_lines.append("WARNING: FBref skipped — p60_gstart/p90_gstart carried forward")
        _checkpoint("FBref done")

        # Step 5b
        if job_id and is_cancelled(job_id):
            return
        if not skip_tm and driver:
            absence_df = fetch_transfermarkt_injuries(existing_players, driver, limit=limit, _job_id=job_id, force_all=force_all_tm)
        else:
            reason = "--skip-tm" if skip_tm else "no browser"
            job_log(f"Skipping Transfermarkt ({reason})")
            absence_df = None
            log_lines.append("WARNING: Transfermarkt skipped — player_absence_history.csv unchanged")
        _checkpoint("Transfermarkt done")

        # Step 6
        new_players = compute_player_priors(existing_players, fotmob_stats, fbref_stats, fpl_players)
        new_players, team_affiliation_log = refresh_team_affiliation(new_players, fpl_players, fpl_teams, existing_teams)
        new_teams   = compute_team_priors(existing_teams, team_raw, fpl_teams)

        log_lines += team_affiliation_log

        log_lines.append("\nCarry-forward columns (no automated source):")
        for col in ["on_pens", "tack_w", "goal_oa", "market_value_€m"]:
            log_lines.append(f"  {col}")

        log_lines += flag_new_players(fpl_players, fpl_teams, existing_players)

        # Step 7
        write_outputs(new_players, new_teams, player_cols, team_cols, log_lines, absence_df)

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
            job_log("Browser closed")
