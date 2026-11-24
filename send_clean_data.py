"""Publish cleaned football CSV rows to Kafka as JSON records."""

import json
import time

import pandas as pd
from kafka import KafkaProducer


MATCHES_PATH = "Cleaned data/matches.csv"
TEAMS_PATH = "Cleaned data/teams.csv"
EVENTS_PATH = "Cleaned data/events.csv"
KAFKA_TOPIC = "matches"

# Only these columns are allowed to be missing in the cleaned source files.
OPTIONAL_FIELDS = {
    "position",       # teams.csv
    "event_desc",     # events.csv
    "player_id",      # events.csv
    "player_name",    # events.csv
    "assist_id",      # events.csv
    "assist_name",    # events.csv
}

# Keep the same pacing as the original producer.
EVENT_DELAY_SECONDS = 3
GAME_START_DELAY_SECONDS = 10
MATCH_END_DELAY_SECONDS = 5


def is_missing(value) -> bool:
    """Return whether one scalar Pandas value is missing."""
    return bool(pd.isna(value))


def prepare_record(record: dict) -> dict:
    """Encode allowed missing cells as JSON null and preserve zero values."""
    for name in OPTIONAL_FIELDS:
        if name in record and is_missing(record[name]):
            record[name] = None

    unexpected_missing = [
        name
        for name, value in record.items()
        if name not in OPTIONAL_FIELDS and is_missing(value)
    ]
    if unexpected_missing:
        raise ValueError(
            "Unexpected missing value(s) in required field(s): "
            + ", ".join(unexpected_missing)
        )
    return record


def create_producer() -> KafkaProducer:
    return KafkaProducer(bootstrap_servers="localhost:9092")


def rows_for_match(grouped, match_id) -> pd.DataFrame:
    """Get one match group without materializing the full CSV as dictionaries."""
    try:
        return grouped.get_group(match_id)
    except KeyError:
        return pd.DataFrame()


def send_record(producer: KafkaProducer, record: dict) -> None:
    """Send one JSON record, keeping all records for a match in Kafka order."""
    payload = json.dumps(prepare_record(record), allow_nan=False).encode("utf-8")
    match_id = record.get("match_id")
    key = None if is_missing(match_id) else str(match_id).encode("utf-8")
    future = producer.send(KAFKA_TOPIC, value=payload, key=key)
    if record.get("record_type") == "MATCH_END":
        # Wait for Kafka's acknowledgement before starting the next match.
        # Since the same match key is used for all its records, acknowledgement
        # also confirms this end marker followed that match's events in order.
        future.get(timeout=30)
        print(f"Match-end marker delivered to Kafka for match {match_id}", flush=True)


def send_clean_data(producer: KafkaProducer) -> None:
    print("Loading cleaned CSV files...")
    matches = pd.read_csv(MATCHES_PATH).sort_values("match_id")
    teams_by_match = pd.read_csv(TEAMS_PATH).groupby("match_id", sort=False)
    events_by_match = pd.read_csv(EVENTS_PATH).groupby("match_id", sort=False)
    print(
        "CSV files are ready; starting the replay for "
        f"{len(matches):,} matches on topic {KAFKA_TOPIC!r}."
    )

    sent = 0
    next_progress = 10000
    for match in matches.to_dict(orient="records"):
        match_id = match["match_id"]
        send_record(producer, match)
        sent += 1

        team_rows = rows_for_match(teams_by_match, match_id)
        for team in team_rows.to_dict(orient="records"):
            send_record(producer, team)
            sent += 1

        print("################# game started!! wait 10 seconds #################")
        time.sleep(GAME_START_DELAY_SECONDS)

        event_rows = rows_for_match(events_by_match, match_id)
        for event in event_rows.to_dict(orient="records"):
            send_record(producer, event)
            sent += 1
            print("Event sent to Kafka")
            print("################# wait 3 seconds #################")
            time.sleep(EVENT_DELAY_SECONDS)

        # Explicit control record: Spark prints this match's report only after
        # every roster and event record for this match has arrived.
        send_record(producer, {"record_type": "MATCH_END", "match_id": match_id})
        sent += 1
        print("################# match ended; wait 5 seconds #################")
        time.sleep(MATCH_END_DELAY_SECONDS)

        while sent >= next_progress:
            print(f"Queued {sent:,} records")
            next_progress += 10000

    producer.flush()
    print(f"Finished: sent {sent:,} records to Kafka topic {KAFKA_TOPIC!r}")


if __name__ == "__main__":
    kafka_producer = create_producer()
    try:
        send_clean_data(kafka_producer)
    finally:
        kafka_producer.close()
