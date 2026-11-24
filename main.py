"""Live event output and final football analyses from Kafka Structured Streaming."""

import json
import math
import re
import sqlite3
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StringType, StructField, StructType

from data_reader import KAFKA_TOPIC, read_kafka_stream, spark_session


# Schemas reflect the files in Cleaned data. Missing values are allowed only
# in these columns; zeros remain ordinary values.
OPTIONAL_FIELDS = {
    "MATCHES": set(),
    "TEAMS": {"position"},
    "EVENTS": {"event_desc", "player_id", "player_name", "assist_id", "assist_name"},
}


def field(name: str, table: str) -> StructField:
    return StructField(name, StringType(), name in OPTIONAL_FIELDS[table])


SCHEMAS = {
    "MATCHES": StructType([
        field("match_id", "MATCHES"), field("season", "MATCHES"),
        field("game_week", "MATCHES"), field("kickoff", "MATCHES"),
        field("home", "MATCHES"), field("home_id", "MATCHES"),
        field("home_score", "MATCHES"), field("home_ht_score", "MATCHES"),
        field("away", "MATCHES"), field("away_id", "MATCHES"),
        field("away_score", "MATCHES"), field("away_ht_score", "MATCHES"),
    ]),
    "TEAMS": StructType([
        field("match_id", "TEAMS"), field("team_id", "TEAMS"),
        field("field", "TEAMS"), field("type", "TEAMS"),
        field("player_id", "TEAMS"), field("player_name", "TEAMS"),
        field("position", "TEAMS"),
    ]),
    "EVENTS": StructType([
        field("match_id", "EVENTS"), field("event_id", "EVENTS"),
        field("half", "EVENTS"), field("event_type", "EVENTS"),
        field("event_desc", "EVENTS"), field("team_id", "EVENTS"),
        field("player_id", "EVENTS"), field("player_name", "EVENTS"),
        field("assist_id", "EVENTS"), field("assist_name", "EVENTS"),
        field("minute", "EVENTS"), field("seconds", "EVENTS"),
    ]),
}

GOAL_TYPES = {"Goal", "Penalty Scored", "Own-Goal"}
YELLOW_TYPES = {"Yellow Card", "Second Yellow"}
RED_TYPES = {"Red Card", "Second Yellow"}

# Isolated fresh state for per-match reports. This avoids resuming an older
# checkpoint/database that could skip a replay's duplicate records.
CHECKPOINT_LOCATION = "checkpoints/football-event-live-v4-stream"
STATE_DATABASE = Path("checkpoints/football-event-live-v4-state.sqlite")

ANALYSIS_STATE = None
STATE_CONNECTION = None


def empty_state() -> dict:
    return {
        "last_offsets": {},
        "matches": {},
        "pending_matches": {},
        "team_names": {},
        "player_names": {},
        "player_teams": {},
        "goals_by_half": {"1": 0, "2": 0},
        "goals_first_15": 0,
        "goals_last_15": 0,
        "goals_home_away": {"Home": 0, "Away": 0},
        "team_goals_home": {},
        "team_goals_away": {},
        "goal_details": [],
        "player_goals": {},
        "player_assists": {},
        "team_goals_for": {},
        "team_goals_against": {},
        "team_wdl": {},
        "player_yellows": {},
        "player_reds": {},
        "team_cards": {},
        "team_yellows": {},
        "team_reds": {},
        "cards_home_away": {
            "Home": {"Yellow": 0, "Red": 0, "Total": 0},
            "Away": {"Yellow": 0, "Red": 0, "Total": 0},
        },
        "team_cards_by_side": {},
        "processed_matches": 0,
        "processed_events": 0,
        "unrecognized_records": 0,
    }


def open_state_database():
    """Open a tiny local store for restart-safe counters and de-duplication."""
    global ANALYSIS_STATE, STATE_CONNECTION
    STATE_DATABASE.parent.mkdir(parents=True, exist_ok=True)
    STATE_CONNECTION = sqlite3.connect(str(STATE_DATABASE), check_same_thread=False)
    STATE_CONNECTION.execute("PRAGMA journal_mode=WAL")
    STATE_CONNECTION.execute(
        "CREATE TABLE IF NOT EXISTS analysis_state "
        "(id INTEGER PRIMARY KEY CHECK (id = 1), payload TEXT NOT NULL)"
    )
    STATE_CONNECTION.execute(
        "CREATE TABLE IF NOT EXISTS seen_records "
        "(kind TEXT NOT NULL, record_key TEXT NOT NULL, "
        "PRIMARY KEY (kind, record_key))"
    )
    row = STATE_CONNECTION.execute(
        "SELECT payload FROM analysis_state WHERE id = 1"
    ).fetchone()
    if row:
        ANALYSIS_STATE = json.loads(row[0])
    else:
        ANALYSIS_STATE = empty_state()
        STATE_CONNECTION.execute(
            "INSERT INTO analysis_state (id, payload) VALUES (1, ?)",
            (json.dumps(ANALYSIS_STATE),),
        )
        STATE_CONNECTION.commit()


def save_state() -> None:
    STATE_CONNECTION.execute(
        "UPDATE analysis_state SET payload = ? WHERE id = 1",
        (json.dumps(ANALYSIS_STATE, separators=(",", ":")),),
    )


def is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"", "nan", "null", "none"}
    if isinstance(value, float):
        return math.isnan(value)
    return False


def clean_text(value):
    return None if is_missing(value) else str(value).strip()


def id_key(value):
    if is_missing(value):
        return None
    text = str(value).strip()
    try:
        number = float(text)
        if number.is_integer():
            return str(int(number))
    except (TypeError, ValueError):
        pass
    return text


def integer(value):
    if is_missing(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def increment(counter: dict, key, amount: int = 1) -> None:
    if key is not None:
        string_key = str(key)
        counter[string_key] = int(counter.get(string_key, 0)) + amount


def entity_key(identifier, name):
    normalized_id = id_key(identifier)
    if normalized_id is not None:
        return normalized_id
    cleaned_name = clean_text(name)
    return f"name:{cleaned_name.casefold()}" if cleaned_name else None


def entity_label(identifier, name, fallback: str) -> tuple:
    key = entity_key(identifier, name)
    label = clean_text(name)
    if not label and id_key(identifier) is not None:
        label = f"{fallback} {id_key(identifier)}"
    return key, label or fallback


def remember_player(identifier, name, fallback: str, team_id=None) -> str:
    key, label = entity_label(identifier, name, fallback)
    if key is not None:
        ANALYSIS_STATE["player_names"][key] = label
        normalized_team = id_key(team_id)
        if normalized_team is not None:
            ANALYSIS_STATE["player_teams"][key] = normalized_team
    return key


def remember_team(identifier, name) -> str:
    key = id_key(identifier)
    label = clean_text(name)
    if key is not None and label:
        ANALYSIS_STATE["team_names"][key] = label
    return key


def team_label(team_id) -> str:
    key = id_key(team_id)
    return ANALYSIS_STATE["team_names"].get(key, f"Team {key or 'Unknown'}")


def first_seen(kind: str, record_key: str) -> bool:
    cursor = STATE_CONNECTION.execute(
        "INSERT OR IGNORE INTO seen_records (kind, record_key) VALUES (?, ?)",
        (kind, record_key),
    )
    return cursor.rowcount == 1


def valid_match(record: dict) -> bool:
    required = (
        "match_id", "home", "home_id", "home_score",
        "away", "away_id", "away_score",
    )
    return all(not is_missing(record.get(name)) for name in required)


def handle_match(record: dict) -> None:
    match_id = id_key(record.get("match_id"))
    home_id = remember_team(record.get("home_id"), record.get("home"))
    away_id = remember_team(record.get("away_id"), record.get("away"))
    home_score = integer(record.get("home_score"))
    away_score = integer(record.get("away_score"))
    if not all((match_id, home_id, away_id)) or home_score is None or away_score is None:
        ANALYSIS_STATE["unrecognized_records"] += 1
        return

    ANALYSIS_STATE["matches"][match_id] = {
        "home_id": home_id,
        "away_id": away_id,
        "home": clean_text(record.get("home")),
        "away": clean_text(record.get("away")),
        "home_score": home_score,
        "away_score": away_score,
    }
    ANALYSIS_STATE["pending_matches"].setdefault(
        match_id, {"roster": {}, "events": []}
    )
    wdl = ANALYSIS_STATE["team_wdl"]
    wdl.setdefault(home_id, {"wins": 0, "draws": 0, "losses": 0})
    wdl.setdefault(away_id, {"wins": 0, "draws": 0, "losses": 0})
    if home_score > away_score:
        wdl[home_id]["wins"] += 1
        wdl[away_id]["losses"] += 1
    elif home_score < away_score:
        wdl[home_id]["losses"] += 1
        wdl[away_id]["wins"] += 1
    else:
        wdl[home_id]["draws"] += 1
        wdl[away_id]["draws"] += 1
    ANALYSIS_STATE["processed_matches"] += 1


def match_side(match: dict, team_id: str):
    if not match or not team_id:
        return None
    if team_id == match["home_id"]:
        return "Home"
    if team_id == match["away_id"]:
        return "Away"
    return None


def handle_team(record: dict) -> None:
    match_id = id_key(record.get("match_id"))
    player_id = id_key(record.get("player_id"))
    player_name = clean_text(record.get("player_name"))
    if not match_id or not (player_id or player_name):
        return
    pending = ANALYSIS_STATE["pending_matches"].setdefault(
        match_id, {"roster": {}, "events": []}
    )
    player_key = player_id or f"name:{player_name.casefold()}"
    pending["roster"][player_key] = {
        "player_id": player_id,
        "player_name": player_name,
        "position": clean_text(record.get("position")) or "Unknown",
        "team_id": id_key(record.get("team_id")),
        "field": clean_text(record.get("field")),
    }


def minute_number(value):
    text = clean_text(value)
    if not text:
        return None
    found = re.match(r"\s*(\d+)", text)
    return int(found.group(1)) if found else None


def valid_event(record: dict) -> bool:
    required = ("match_id", "event_id", "half", "event_type", "team_id", "minute", "seconds")
    return all(not is_missing(record.get(name)) for name in required)


def handle_goal(record: dict, match: dict) -> None:
    event_type = clean_text(record.get("event_type"))
    half = id_key(record.get("half"))
    if half in {"1", "2"}:
        increment(ANALYSIS_STATE["goals_by_half"], half)

    minute = minute_number(record.get("minute"))
    if minute is not None:
        if minute <= 15:
            ANALYSIS_STATE["goals_first_15"] += 1
        if minute >= 76:
            ANALYSIS_STATE["goals_last_15"] += 1

    scoring_team = id_key(record.get("team_id"))
    scoring_side = match_side(match, scoring_team)
    credited_team = scoring_team
    conceded_team = None
    credited_side = scoring_side

    if match and scoring_side:
        opponent_side = "Away" if scoring_side == "Home" else "Home"
        opponent_id = match[f"{opponent_side.lower()}_id"]
        if event_type == "Own-Goal":
            credited_team = opponent_id
            conceded_team = scoring_team
            credited_side = opponent_side
        else:
            conceded_team = opponent_id

    increment(ANALYSIS_STATE["team_goals_for"], credited_team)
    increment(ANALYSIS_STATE["team_goals_against"], conceded_team)
    increment(ANALYSIS_STATE["goals_home_away"], credited_side)
    if credited_side == "Home":
        increment(ANALYSIS_STATE["team_goals_home"], credited_team)
    elif credited_side == "Away":
        increment(ANALYSIS_STATE["team_goals_away"], credited_team)

    match_id = id_key(record.get("match_id"))
    pending = ANALYSIS_STATE["pending_matches"].get(match_id, {"roster": {}})
    scorer = roster_person(pending, record.get("player_id"), record.get("player_name"))
    ANALYSIS_STATE["goal_details"].append({
        "Half": "First Half" if half == "1" else "Second Half" if half == "2" else half or "Unknown",
        "Minute": clean_text(record.get("minute")) or "Unknown",
        "Player ID": scorer["player_id"], "Player": scorer["player_name"],
        "Team": team_label(scoring_team), "Position": scorer["position"],
        "Goal Type": event_type,
    })

    if event_type in {"Goal", "Penalty Scored"}:
        player_key = remember_player(
            record.get("player_id"), record.get("player_name"), "Player",
            record.get("team_id"),
        )
        increment(ANALYSIS_STATE["player_goals"], player_key)

    if event_type != "Own-Goal":
        assist_id = record.get("assist_id")
        assist_name = record.get("assist_name")
        if not is_missing(assist_id) or not is_missing(assist_name):
            assist_key = remember_player(assist_id, assist_name, "Player", record.get("team_id"))
            increment(ANALYSIS_STATE["player_assists"], assist_key)


def handle_card(record: dict, match: dict) -> None:
    team_id = id_key(record.get("team_id"))
    player_key = remember_player(
        record.get("player_id"), record.get("player_name"), "Player", team_id
    )
    description = clean_text(record.get("event_desc"))
    is_yellow = description in YELLOW_TYPES
    is_red = description in RED_TYPES

    # Each Booking event counts as one team card, including rows whose
    # optional event_desc is absent. A second yellow counts for both rankings.
    increment(ANALYSIS_STATE["team_cards"], team_id)
    if is_yellow:
        increment(ANALYSIS_STATE["team_yellows"], team_id)
        increment(ANALYSIS_STATE["player_yellows"], player_key)
    if is_red:
        increment(ANALYSIS_STATE["team_reds"], team_id)
        increment(ANALYSIS_STATE["player_reds"], player_key)

    side = match_side(match, team_id)
    if side:
        side_counts = ANALYSIS_STATE["cards_home_away"][side]
        side_counts["Total"] += 1
        if is_yellow:
            side_counts["Yellow"] += 1
        if is_red:
            side_counts["Red"] += 1
        per_team_side = ANALYSIS_STATE["team_cards_by_side"].setdefault(
            str(team_id), {"Home Yellow": 0, "Away Yellow": 0,
                           "Home Red": 0, "Away Red": 0}
        )
        if is_yellow:
            per_team_side[f"{side} Yellow"] += 1
        if is_red:
            per_team_side[f"{side} Red"] += 1


def process_event(record: dict) -> None:
    match_id = id_key(record.get("match_id"))
    event_type = clean_text(record.get("event_type")) or "Unknown"
    match = ANALYSIS_STATE["matches"].get(match_id)
    pending = ANALYSIS_STATE["pending_matches"].setdefault(
        match_id, {"roster": {}, "events": []}
    )
    pending["events"].append(record)
    if event_type in GOAL_TYPES:
        handle_goal(record, match)
    elif event_type == "Booking":
        handle_card(record, match)
    ANALYSIS_STATE["processed_events"] += 1


def live_event_line(record: dict) -> str:
    match_id = id_key(record.get("match_id")) or "Unknown"
    event_type = clean_text(record.get("event_type")) or "Unknown event"
    pending = ANALYSIS_STATE["pending_matches"].get(match_id, {"roster": {}})
    player = roster_person(pending, record.get("player_id"), record.get("player_name"))
    team = team_label(record.get("team_id"))
    minute = clean_text(record.get("minute")) or "Unknown"
    description = clean_text(record.get("event_desc"))
    event_label = f"{event_type} ({description})" if description and description != event_type else event_type
    assist_name = clean_text(record.get("assist_name"))
    assist = f" | Assist: {assist_name}" if assist_name else ""
    return (
        f"[LIVE EVENT] Match {match_id} | {minute}' | Team: {team} | "
        f"Player: {player['player_name']} | Event: {event_label}{assist}"
    )


def roster_person(pending: dict, identifier, name) -> dict:
    player_id = id_key(identifier)
    player_name = clean_text(name)
    roster = pending.get("roster", {})
    person = roster.get(player_id) if player_id else None
    if person is None and player_name:
        person = roster.get(f"name:{player_name.casefold()}")
    person = person or {}
    return {
        "player_id": player_id or person.get("player_id") or "Unknown",
        "player_name": player_name or person.get("player_name") or "Unknown",
        "position": person.get("position") or "Unknown",
    }


def build_match_report(match_id: str):
    pending = ANALYSIS_STATE["pending_matches"].get(match_id)
    match = ANALYSIS_STATE["matches"].get(match_id)
    if pending is None or match is None:
        return None

    events = pending.get("events", [])
    goals = [event for event in events if clean_text(event.get("event_type")) in GOAL_TYPES]
    bookings = [event for event in events if clean_text(event.get("event_type")) == "Booking"]
    home_id, away_id = match["home_id"], match["away_id"]
    teams = [home_id, away_id]
    goals_for = {team_id: 0 for team_id in teams}
    goals_against = {team_id: 0 for team_id in teams}
    goals_by_side = {"Home": 0, "Away": 0}
    half_rows, first_15_rows, last_15_rows = [], [], []
    assist_rows = []
    scorer_totals, assist_totals = {}, {}

    for event in goals:
        event_type = clean_text(event.get("event_type")) or "Unknown"
        half = id_key(event.get("half")) or "Unknown"
        minute_text = clean_text(event.get("minute")) or "Unknown"
        minute = minute_number(minute_text)
        scoring_team = id_key(event.get("team_id"))
        scoring_side = match_side(match, scoring_team)
        credited_team = scoring_team
        if event_type == "Own-Goal" and scoring_side:
            opposite = "Away" if scoring_side == "Home" else "Home"
            credited_team = match[f"{opposite.lower()}_id"]
        credited_side = match_side(match, credited_team)
        if credited_side:
            goals_by_side[credited_side] += 1
        if credited_team in goals_for:
            goals_for[credited_team] += 1
        if event_type == "Own-Goal":
            if scoring_team in goals_against:
                goals_against[scoring_team] += 1
        elif scoring_side:
            opponent = away_id if scoring_side == "Home" else home_id
            goals_against[opponent] += 1

        scorer = roster_person(pending, event.get("player_id"), event.get("player_name"))
        row = {
            "Half": "First" if half == "1" else "Second" if half == "2" else half,
            "Minute": minute_text,
            "Player ID": scorer["player_id"],
            "Scorer": scorer["player_name"],
            "Team": team_label(scoring_team),
            "Position": scorer["position"],
            "Goal Type": event_type,
        }
        half_rows.append(row)
        if minute is not None and minute <= 15:
            first_15_rows.append(row)
        if minute is not None and minute >= 76:
            last_15_rows.append(row)

        if event_type != "Own-Goal":
            scorer_key = (entity_key(event.get("player_id"), event.get("player_name")), scoring_team)
            if scorer_key[0] is not None:
                item = scorer_totals.setdefault(scorer_key, {
                    "Player ID": scorer["player_id"], "Player": scorer["player_name"],
                    "Team": team_label(scoring_team), "Goals": 0,
                })
                item["Goals"] += 1

            assist_id, assist_name = event.get("assist_id"), event.get("assist_name")
            if not is_missing(assist_id) or not is_missing(assist_name):
                assister = roster_person(pending, assist_id, assist_name)
                assist_rows.append({
                    "Minute": minute_text, "Scorer": scorer["player_name"],
                    "Assist ID": assister["player_id"], "Assist Player": assister["player_name"],
                    "Assist Team": team_label(scoring_team),
                    "Assist Position": assister["position"],
                })
                assist_key = (entity_key(assist_id, assist_name), scoring_team)
                if assist_key[0] is not None:
                    item = assist_totals.setdefault(assist_key, {
                        "Assist ID": assister["player_id"], "Player": assister["player_name"],
                        "Team": team_label(scoring_team), "Assists": 0,
                    })
                    item["Assists"] += 1

    yellow_events = [e for e in bookings if clean_text(e.get("event_desc")) in YELLOW_TYPES]
    red_events = [e for e in bookings if clean_text(e.get("event_desc")) in RED_TYPES]
    yellow_totals, red_totals = {}, {}
    team_cards = {team_id: {"Yellow": 0, "Red": 0, "Total": 0} for team_id in teams}
    side_cards = {
        "Home": {"Team": match["home"], "Yellow": 0, "Red": 0, "Total": 0},
        "Away": {"Team": match["away"], "Yellow": 0, "Red": 0, "Total": 0},
    }

    def card_detail(event):
        person = roster_person(pending, event.get("player_id"), event.get("player_name"))
        team_id = id_key(event.get("team_id"))
        return {
            "Minute": clean_text(event.get("minute")) or "Unknown",
            "Player ID": person["player_id"], "Player": person["player_name"],
            "Position": person["position"], "Team": team_label(team_id),
            "Card": clean_text(event.get("event_desc")) or "Unspecified Booking",
        }

    yellow_rows, red_rows = [], []
    for event in bookings:
        detail = card_detail(event)
        team_id = id_key(event.get("team_id"))
        description = clean_text(event.get("event_desc"))
        side = match_side(match, team_id)
        if team_id in team_cards:
            team_cards[team_id]["Total"] += 1
        if side:
            side_cards[side]["Total"] += 1
        if description in YELLOW_TYPES:
            yellow_rows.append(detail)
            bucket = yellow_totals
            kind = "Yellow"
            if team_id in team_cards:
                team_cards[team_id]["Yellow"] += 1
            if side:
                side_cards[side]["Yellow"] += 1
        elif description in RED_TYPES:
            red_rows.append(detail)
            bucket = red_totals
            kind = "Red"
            if team_id in team_cards:
                team_cards[team_id]["Red"] += 1
            if side:
                side_cards[side]["Red"] += 1
        else:
            continue
        player_key = (entity_key(event.get("player_id"), event.get("player_name")), team_id)
        if player_key[0] is not None:
            person = roster_person(pending, event.get("player_id"), event.get("player_name"))
            item = bucket.setdefault(player_key, {
                "Player ID": person["player_id"], "Player": person["player_name"],
                "Team": team_label(team_id), f"{kind} Cards": 0,
            })
            item[f"{kind} Cards"] += 1

    home_score, away_score = match["home_score"], match["away_score"]
    if home_score > away_score:
        home_result, away_result = "Win", "Loss"
    elif home_score < away_score:
        home_result, away_result = "Loss", "Win"
    else:
        home_result = away_result = "Draw"
    wdl_rows = [
        {"Team": match["home"], "Result": home_result},
        {"Team": match["away"], "Result": away_result},
    ]
    team_goal_rows = [{"Team": match["home" if team_id == home_id else "away"], "Goals": goals_for[team_id]}
                      for team_id in teams]
    conceded_rows = [{"Team": match["home" if team_id == home_id else "away"], "Goals Conceded": goals_against[team_id]}
                     for team_id in teams]
    team_card_rows = [
        {"Team": match["home" if team_id == home_id else "away"],
         "Yellow Cards": team_cards[team_id]["Yellow"],
         "Red Cards": team_cards[team_id]["Red"],
         "Total Cards": team_cards[team_id]["Total"]}
        for team_id in teams
    ]
    cards_side_rows = [
        {"Side": side, "Team": data["Team"], "Yellow Cards": data["Yellow"],
         "Red Cards": data["Red"], "Total Cards": data["Total"]}
        for side, data in side_cards.items()
    ]

    def ranking(counter, field):
        return sorted(counter.values(), key=lambda row: (-row[field], row["Player"].casefold()))[:10]

    return {
        "match_id": match_id,
        "summary": [{"Home": match["home"], "Score": f"{home_score} - {away_score}", "Away": match["away"]}],
        "goals_by_half": half_rows, "first_15": first_15_rows, "last_15": last_15_rows,
        "goals_by_side": [
            {"Side": "Home", "Team": match["home"], "Goals": goals_by_side["Home"]},
            {"Side": "Away", "Team": match["away"], "Goals": goals_by_side["Away"]},
        ],
        "top_scorers": ranking(scorer_totals, "Goals"),
        "top_assists": ranking(assist_totals, "Assists"),
        "team_goals": team_goal_rows, "team_conceded": conceded_rows, "wdl": wdl_rows,
        "top_yellows": ranking(yellow_totals, "Yellow Cards"),
        "top_reds": ranking(red_totals, "Red Cards"),
        "team_cards": team_card_rows, "cards_side": cards_side_rows,
        "assist_events": assist_rows, "yellow_events": yellow_rows, "red_events": red_rows,
    }


def analyze_completed_match(match_id: str):
    """Build the report from this match alone after its end marker arrives."""
    return build_match_report(match_id)


def print_match_report(report: dict) -> None:
    match_id = report["match_id"]
    print(f"\n\n{'#' * 88}\nMATCH {match_id} COMPLETE - PER-MATCH ANALYSIS\n{'#' * 88}", flush=True)

    def show(title, rows, columns):
        schema = StructType([StructField(name, StringType(), True) for name in columns])
        normalized = [{name: str(row.get(name, "")) for name in columns} for row in rows]
        print_table(f"Match {match_id}: {title}", normalized, schema)

    show("Match Result", report["summary"], ["Home", "Score", "Away"])
    goal_cols = ["Half", "Minute", "Player ID", "Scorer", "Team", "Position", "Goal Type"]
    show("1. Goals by Half", report["goals_by_half"], goal_cols)
    show("2. Goals in the First 15 Minutes", report["first_15"], goal_cols)
    show("3. Goals in the Last 15 Minutes (76+)", report["last_15"], goal_cols)
    show("4. Goals Scored by Match Side", report["goals_by_side"], ["Side", "Team", "Goals"])
    show("5. Top Goal Scorers (top 10)", report["top_scorers"], ["Player ID", "Player", "Team", "Goals"])
    show("6. Top Assist Providers (top 10)", report["top_assists"], ["Assist ID", "Player", "Team", "Assists"])
    show("7. Teams with Most Goals Scored", report["team_goals"], ["Team", "Goals"])
    show("8. Teams with Most Goals Conceded", report["team_conceded"], ["Team", "Goals Conceded"])
    show("9. Win, Draw, Loss", report["wdl"], ["Team", "Result"])
    show("10. Players with Most Yellow Cards (top 10)", report["top_yellows"], ["Player ID", "Player", "Team", "Yellow Cards"])
    show("11. Players with Most Red Cards (top 10)", report["top_reds"], ["Player ID", "Player", "Team", "Red Cards"])
    show("12. Teams with Most Cards", report["team_cards"], ["Team", "Yellow Cards", "Red Cards", "Total Cards"])
    show("13. Cards: Home vs Away", report["cards_side"], ["Side", "Team", "Yellow Cards", "Red Cards", "Total Cards"])
    show("Assist Events", report["assist_events"], ["Minute", "Scorer", "Assist ID", "Assist Player", "Assist Team", "Assist Position"])
    card_cols = ["Minute", "Player ID", "Player", "Position", "Team", "Card"]
    show("Yellow Card Events", report["yellow_events"], card_cols)
    show("Red Card Events", report["red_events"], card_cols)
    print(f"Finished analysis for match {match_id}.\n", flush=True)


def as_dict(value):
    return value.asDict(recursive=True) if value is not None else None


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Analyze each event as it arrives and defer final rankings until shutdown."""
    global ANALYSIS_STATE
    live_events = []
    live_matches = []
    rows = (
        batch_df.select(
            col("partition"),
            col("offset"),
            col("value").cast("string").alias("payload"),
            from_json(col("value").cast("string"), SCHEMAS["MATCHES"]).alias("match"),
            from_json(col("value").cast("string"), SCHEMAS["TEAMS"]).alias("team"),
            from_json(col("value").cast("string"), SCHEMAS["EVENTS"]).alias("event"),
        )
        .orderBy("partition", "offset")
        .toLocalIterator()
    )
    malformed = 0
    STATE_CONNECTION.execute("BEGIN IMMEDIATE")
    try:
        for row in rows:
            partition = str(row["partition"])
            offset = int(row["offset"])
            previous_offset = int(ANALYSIS_STATE["last_offsets"].get(partition, -1))
            if offset <= previous_offset:
                continue

            match_record = as_dict(row["match"])
            team_record = as_dict(row["team"])
            event_record = as_dict(row["event"])

            if match_record and not is_missing(match_record.get("season")):
                record_type, record = "MATCHES", match_record
            elif team_record and not is_missing(team_record.get("field")):
                record_type, record = "TEAMS", team_record
            elif event_record and not is_missing(event_record.get("event_id")):
                record_type, record = "EVENTS", event_record
            else:
                record_type, record = None, None

            if record_type is None:
                try:
                    control = json.loads(row["payload"] or "{}")
                except (json.JSONDecodeError, TypeError):
                    control = {}
                if control.get("record_type") == "MATCH_END":
                    match_key = id_key(control.get("match_id"))
                    if not match_key:
                        ANALYSIS_STATE["unrecognized_records"] += 1
                        malformed += 1
                    elif first_seen("MATCH_END", match_key):
                        ANALYSIS_STATE["pending_matches"].pop(match_key, None)
                else:
                    ANALYSIS_STATE["unrecognized_records"] += 1
                    malformed += 1
            elif record_type == "MATCHES" and valid_match(record):
                match_key = id_key(record.get("match_id"))
                if match_key:
                    live_matches.append(
                        f"[MATCH START] Kick-off: {clean_text(record.get('kickoff')) or 'Unknown'} | "
                        f"Home: {clean_text(record.get('home')) or 'Unknown'} | "
                        f"Away: {clean_text(record.get('away')) or 'Unknown'} | "
                        f"Match ID: {match_key}"
                    )
                    if first_seen("MATCHES", match_key):
                        handle_match(record)
            elif record_type == "TEAMS":
                handle_team(record)
            elif record_type == "EVENTS" and valid_event(record):
                event_key = f"{id_key(record.get('match_id'))}:{id_key(record.get('event_id'))}"
                if first_seen("EVENTS", event_key):
                    process_event(record)
                # Print each newly received Kafka event even when its semantic
                # ID was analyzed in an earlier replay. Counters stay deduped.
                live_events.append(live_event_line(record))
            else:
                ANALYSIS_STATE["unrecognized_records"] += 1
                malformed += 1

            ANALYSIS_STATE["last_offsets"][partition] = offset

        save_state()
        STATE_CONNECTION.commit()
    except Exception:
        STATE_CONNECTION.rollback()
        row = STATE_CONNECTION.execute(
            "SELECT payload FROM analysis_state WHERE id = 1"
        ).fetchone()
        ANALYSIS_STATE = json.loads(row[0]) if row else empty_state()
        raise

    for line in live_matches:
        print(line, flush=True)
    for line in live_events:
        print(line, flush=True)
    if malformed:
        print(f"Batch {batch_id}: skipped {malformed} malformed record(s)", flush=True)


def ranked_rows(counter: dict, labels: dict, player_teams: dict, limit: int = 10) -> list:
    ordered = sorted(
        counter.items(),
        key=lambda item: (-int(item[1]), labels.get(item[0], item[0]).casefold()),
    )[:limit]
    return [
        {"Name": labels.get(key, key),
         "Team": team_label(player_teams.get(key)),
         "Count": int(value)}
        for key, value in ordered
    ]


def team_rank_rows(counter: dict, limit: int = 10) -> list:
    ordered = sorted(
        counter.items(),
        key=lambda item: (-int(item[1]), team_label(item[0]).casefold()),
    )[:limit]
    return [{"Team": team_label(key), "Count": int(value)} for key, value in ordered]


def print_table(title: str, rows: list, schema: StructType, limit=None) -> None:
    print(f"\n{'=' * 88}\n{title}\n{'=' * 88}")
    if not rows:
        print("No rows yet.")
        return
    columns = [item.name for item in schema.fields]
    shown_rows = rows[:limit] if limit else rows
    rendered = [[str(row.get(name, "")) for name in columns] for row in shown_rows]
    widths = [
        max(len(name), *(len(row[index]) for row in rendered))
        for index, name in enumerate(columns)
    ]

    def border():
        return "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    def render(values):
        return "| " + " | ".join(
            value.ljust(widths[index]) for index, value in enumerate(values)
        ) + " |"

    print(border())
    print(render(columns))
    print(border())
    for values in rendered:
        print(render(values))
    print(border())


def print_analysis_tables() -> None:
    state = ANALYSIS_STATE
    half_schema = StructType([
        StructField("Half", StringType(), False),
        StructField("Goals", LongType(), False),
    ])
    count_schema = StructType([
        StructField("Measure", StringType(), False),
        StructField("Count", LongType(), False),
    ])
    side_schema = StructType([
        StructField("Side", StringType(), False),
        StructField("Goals", LongType(), False),
    ])
    rank_schema = StructType([
        StructField("Name", StringType(), False),
        StructField("Team", StringType(), False),
        StructField("Count", LongType(), False),
    ])
    team_rank_schema = StructType([
        StructField("Team", StringType(), False),
        StructField("Count", LongType(), False),
    ])
    wdl_schema = StructType([
        StructField("Team", StringType(), False),
        StructField("Wins", LongType(), False),
        StructField("Draws", LongType(), False),
        StructField("Losses", LongType(), False),
    ])
    team_cards_schema = StructType([
        StructField("Team", StringType(), False),
        StructField("Yellow Cards", LongType(), False),
        StructField("Red Cards", LongType(), False),
        StructField("Total Card Events", LongType(), False),
    ])
    side_cards_schema = StructType([
        StructField("Team", StringType(), False),
        StructField("Home Yellow", LongType(), False),
        StructField("Away Yellow", LongType(), False),
        StructField("Home Red", LongType(), False),
        StructField("Away Red", LongType(), False),
    ])

    goal_detail_schema = StructType([
        StructField(name, StringType(), False)
        for name in ("Half", "Minute", "Player ID", "Player", "Team", "Position", "Goal Type")
    ])
    goals = state["goal_details"]
    first_15_goals = [row for row in goals
                      if minute_number(row["Minute"]) is not None
                      and minute_number(row["Minute"]) <= 15]
    last_15_goals = [row for row in goals
                     if minute_number(row["Minute"]) is not None
                     and minute_number(row["Minute"]) >= 76]
    print_table("1. Goals by Half (each goal, scorer, team and position)", goals, goal_detail_schema)
    print_table("2. Goals in the First 15 Minutes", first_15_goals, goal_detail_schema)
    print_table("3. Goals in the Last 15 Minutes (76+)", last_15_goals, goal_detail_schema)
    goal_side_ids = set(state["team_goals_home"]) | set(state["team_goals_away"])
    goal_side_rows = [
        {"Team": team_label(team_id),
         "Home Goals": state["team_goals_home"].get(team_id, 0),
         "Away Goals": state["team_goals_away"].get(team_id, 0),
         "Total Goals": state["team_goals_home"].get(team_id, 0)
                        + state["team_goals_away"].get(team_id, 0)}
        for team_id in goal_side_ids
    ]
    goal_side_rows.sort(key=lambda row: (-row["Total Goals"], row["Team"].casefold()))
    goals_by_side_schema = StructType([
        StructField("Team", StringType(), False),
        StructField("Home Goals", LongType(), False),
        StructField("Away Goals", LongType(), False),
        StructField("Total Goals", LongType(), False),
    ])
    print_table("4. Goals by Team Home/Away Side", goal_side_rows, goals_by_side_schema)
    print_table("5. Top Goal Scorers (top 10)",
                ranked_rows(state["player_goals"], state["player_names"], state["player_teams"]), rank_schema)
    print_table("6. Top Assist Providers (top 10)",
                ranked_rows(state["player_assists"], state["player_names"], state["player_teams"]), rank_schema)
    print_table("7. Teams with Most Goals Scored (top 10)",
                team_rank_rows(state["team_goals_for"]), team_rank_schema)
    print_table("8. Teams with Most Goals Conceded (top 10)",
                team_rank_rows(state["team_goals_against"]), team_rank_schema)

    wdl_rows = [
        {"Team": team_label(team_id), "Wins": counts["wins"],
         "Draws": counts["draws"], "Losses": counts["losses"]}
        for team_id, counts in sorted(
            state["team_wdl"].items(), key=lambda item: team_label(item[0]).casefold()
        )
    ]
    print_table("9. Wins, Draws and Losses by Team", wdl_rows, wdl_schema)
    print_table("10. Players with Most Yellow Cards (top 10)",
                ranked_rows(state["player_yellows"], state["player_names"], state["player_teams"]), rank_schema)
    print_table("11. Players with Most Red Cards (top 10)",
                ranked_rows(state["player_reds"], state["player_names"], state["player_teams"]), rank_schema)

    card_team_ids = set(state["team_cards"]) | set(state["team_yellows"]) | set(state["team_reds"])
    team_card_rows = [
        {"Team": team_label(team_id),
         "Yellow Cards": state["team_yellows"].get(team_id, 0),
         "Red Cards": state["team_reds"].get(team_id, 0),
         "Total Card Events": state["team_cards"].get(team_id, 0)}
        for team_id in card_team_ids
    ]
    team_card_rows.sort(key=lambda row: (-row["Total Card Events"], row["Team"].casefold()))
    print_table("12. Teams with Most Cards (top 10)", team_card_rows,
                team_cards_schema, limit=10)

    side_card_rows = [
        {"Team": team_label(team_id), **counts}
        for team_id, counts in state["team_cards_by_side"].items()
    ]
    side_card_rows.sort(key=lambda row: row["Team"].casefold())
    print_table("13. Cards: Home vs Away by Team", side_card_rows, side_cards_schema)

    print(
        f"\nUnique matches processed: {state['processed_matches']:,}; "
        f"unique events processed: {state['processed_events']:,}; "
        f"unrecognized records: {state['unrecognized_records']:,}."
    )


def main() -> None:
    global STATE_CONNECTION
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    open_state_database()
    kafka_df = read_kafka_stream(spark)

    query = (
        kafka_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="1 second")
        .start()
    )

    print(f"Printing each new event from Kafka topic {KAFKA_TOPIC}.")
    print("When you stop the stream with Ctrl+C, final analysis tables will be printed.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping the stream and printing final analysis tables...")
    finally:
        try:
            if query.isActive:
                query.stop()
        except Exception as exc:
            print(f"Streaming shutdown notice: {exc}")
        print_analysis_tables()
        try:
            spark.stop()
        except Exception:
            pass
        if STATE_CONNECTION is not None:
            STATE_CONNECTION.close()


if __name__ == "__main__":
    main()
