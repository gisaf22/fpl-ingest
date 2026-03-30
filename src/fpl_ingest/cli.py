"""CLI entry point for fpl-ingest.

Fetches FPL API data, transforms it, and stores in SQLite.
"""

from __future__ import annotations

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .client import FPLClient
from .models import (
    PlayerModel,
    TeamModel,
    FixtureModel,
    FixtureStatModel,
    GameweekModel,
    EventModel,
    ElementTypeModel,
    PhaseModel,
    ExplainStatModel,
    PlayerHistoryModel,
)
from .store import SQLiteStore
from .transforms import (
    flatten_live_elements,
    flatten_fixture_stats,
    flatten_explain,
    flatten_event,
    flatten_player_history_past,
)

DEFAULT_DB = Path.home() / "Documents" / "FPL" / "data" / "fpl" / "fpl.db"
DEFAULT_RAW_DIR = Path.home() / "Documents" / "FPL" / "data" / "fpl" / "raw"


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the fpl-ingest CLI."""
    parser = argparse.ArgumentParser(
        prog="fpl-ingest",
        description="Collect and store FPL API data.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help=f"SQLite database path (default: {DEFAULT_DB}).",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help=f"Directory for raw JSON cache (default: {DEFAULT_RAW_DIR}).",
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Re-fetch gameweek data even if already cached.",
    )
    parser.add_argument(
        "--skip-history",
        action="store_true",
        help="Skip fetching per-player element-summary history.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """Entry point for the fpl-ingest CLI. Fetches FPL API data and stores to SQLite."""
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger("fpl_ingest")

    db_path = args.db or DEFAULT_DB
    raw_dir = args.raw_dir or DEFAULT_RAW_DIR
    raw_dir.mkdir(parents=True, exist_ok=True)

    client = FPLClient()
    store = SQLiteStore(db_path)

    # Register all tables
    store.register_table("players", PlayerModel)
    store.register_table("teams", TeamModel)
    store.register_table("fixtures", FixtureModel)
    store.register_table("fixture_stats", FixtureStatModel,
                         unique_constraint=FixtureStatModel.DEFAULT_UNIQUE)
    store.register_table("gameweeks", GameweekModel,
                         unique_constraint=GameweekModel.DEFAULT_UNIQUE)
    store.register_table("events", EventModel)
    store.register_table("element_types", ElementTypeModel)
    store.register_table("phases", PhaseModel)
    store.register_table("explain_stats", ExplainStatModel,
                         unique_constraint=ExplainStatModel.DEFAULT_UNIQUE)
    store.register_table("player_history", PlayerHistoryModel,
                         unique_constraint=PlayerHistoryModel.DEFAULT_UNIQUE)

    # Create useful indexes
    store.create_index("gameweeks", ["element_id"])
    store.create_index("gameweeks", ["round"])
    store.create_index("fixtures", ["event"])
    store.create_index("fixture_stats", ["fixture_id"])
    store.create_index("fixture_stats", ["element"])
    store.create_index("explain_stats", ["element_id", "round"])
    store.create_index("player_history", ["element_id"])

    # ── Fetch bootstrap-static ────────────────────────────────────
    logger.info("Fetching bootstrap-static data...")
    bootstrap = client.get_bootstrap()

    # Save raw bootstrap
    bootstrap_path = raw_dir / "bootstrap.json"
    bootstrap_path.write_text(
        json.dumps(bootstrap, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Upsert players
    players = bootstrap.get("elements", [])
    ins, skip = store.upsert_models("players", PlayerModel, players)
    logger.info("Players: %d upserted, %d skipped", ins, skip)

    # Upsert teams
    teams = bootstrap.get("teams", [])
    ins, skip = store.upsert_models("teams", TeamModel, teams)
    logger.info("Teams: %d upserted, %d skipped", ins, skip)

    # Upsert events (gameweek metadata)
    events = bootstrap.get("events", [])
    event_dicts = [flatten_event(e) for e in events]
    ins, skip = store.upsert_models("events", EventModel, event_dicts)
    logger.info("Events: %d upserted, %d skipped", ins, skip)

    # Upsert element types
    element_types = bootstrap.get("element_types", [])
    ins, skip = store.upsert_models("element_types", ElementTypeModel, element_types)
    logger.info("Element types: %d upserted, %d skipped", ins, skip)

    # Upsert phases
    phases = bootstrap.get("phases", [])
    ins, skip = store.upsert_models("phases", PhaseModel, phases)
    logger.info("Phases: %d upserted, %d skipped", ins, skip)

    # ── Fetch fixtures ────────────────────────────────────────────
    logger.info("Fetching fixtures...")
    fixtures = client.get_fixtures()
    if fixtures:
        fixtures_path = raw_dir / "fixtures.json"
        fixtures_path.write_text(
            json.dumps(fixtures, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        ins, skip = store.upsert_models("fixtures", FixtureModel, fixtures)
        logger.info("Fixtures: %d upserted, %d skipped", ins, skip)

        # Upsert fixture stats (goals, assists, cards, etc. per player)
        all_fstats: list[dict] = []
        for fix in fixtures:
            all_fstats.extend(flatten_fixture_stats(fix))
        if all_fstats:
            ins, skip = store.upsert_models("fixture_stats", FixtureStatModel, all_fstats)
            logger.info("Fixture stats: %d upserted, %d skipped", ins, skip)
    else:
        logger.warning("No fixture data returned")

    # ── Fetch gameweek live data ──────────────────────────────────
    finished_gws = [e["id"] for e in events if e.get("finished")]
    logger.info("Found %d finished gameweeks", len(finished_gws))

    # Filter already-cached gameweeks
    if not args.force:
        finished_gws = [
            gw for gw in finished_gws
            if not (raw_dir / f"gw_{gw}.json").exists()
        ]

    if finished_gws:
        logger.info("Collecting %d gameweeks...", len(finished_gws))

        downloaded = 0
        errors = 0

        for i, gw in enumerate(finished_gws, 1):
            try:
                gw_data = client.get_gw(gw)

                if not gw_data:
                    logger.warning("[%d/%d] No data for GW%d", i, len(finished_gws), gw)
                    errors += 1
                    continue

                # Save raw JSON
                gw_path = raw_dir / f"gw_{gw}.json"
                gw_path.write_text(
                    json.dumps(gw_data, ensure_ascii=False, indent=2), encoding="utf-8"
                )

                # Flatten live elements and upsert
                elements = gw_data.get("elements", [])
                flat = flatten_live_elements(elements, gw)

                if flat:
                    ins, skip = store.upsert_models("gameweeks", GameweekModel, flat)
                    logger.debug("  GW%d: %d upserted, %d skipped", gw, ins, skip)

                # Flatten explain data and upsert
                all_explain: list[dict] = []
                for elem in elements:
                    all_explain.extend(flatten_explain(elem, gw))
                if all_explain:
                    ins, skip = store.upsert_models("explain_stats", ExplainStatModel, all_explain)
                    logger.debug("  GW%d explain: %d upserted, %d skipped", gw, ins, skip)

                downloaded += 1
                logger.info(
                    "[%d/%d] GW%d — %d player entries, %d explain rows",
                    i, len(finished_gws), gw, len(flat), len(all_explain),
                )

            except Exception as e:
                errors += 1
                logger.error("[%d/%d] Failed GW%d: %s", i, len(finished_gws), gw, e)

        logger.info("Gameweeks: %d collected, %d errors", downloaded, errors)
    else:
        logger.info("All finished gameweeks already collected.")

    # ── Fetch per-player element-summary (history) ────────────────
    if not args.skip_history:
        player_ids = [p["id"] for p in players]
        history_dir = raw_dir / "players"
        history_dir.mkdir(parents=True, exist_ok=True)

        # Filter already-cached players
        if not args.force:
            player_ids = [
                pid for pid in player_ids
                if not (history_dir / f"{pid}.json").exists()
            ]

        if player_ids:
            logger.info("Fetching element-summary for %d players...", len(player_ids))
            fetched = 0
            errors = 0

            def _fetch_player(pid: int) -> tuple[int, dict | None]:
                return pid, client.get_player_history(pid)

            with ThreadPoolExecutor(max_workers=20) as pool:
                futures = {pool.submit(_fetch_player, pid): pid for pid in player_ids}
                for i, future in enumerate(as_completed(futures), 1):
                    pid = futures[future]
                    try:
                        _, data = future.result()
                        if not data:
                            errors += 1
                            continue

                        # Save raw JSON
                        (history_dir / f"{pid}.json").write_text(
                            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
                        )

                        # Upsert GW-level history rows
                        history = data.get("history", [])
                        if history:
                            ins, skip = store.upsert_models("gameweeks", GameweekModel, history)
                            logger.debug("  Player %d history: %d upserted, %d skipped", pid, ins, skip)

                        # Upsert past-season history
                        history_past = data.get("history_past", [])
                        if history_past:
                            past_dicts = flatten_player_history_past(history_past, pid)
                            ins, skip = store.upsert_models("player_history", PlayerHistoryModel, past_dicts)
                            logger.debug("  Player %d past seasons: %d upserted, %d skipped", pid, ins, skip)

                        fetched += 1
                        if i % 50 == 0:
                            logger.info("[%d/%d] Player histories fetched...", i, len(player_ids))

                    except Exception as e:
                        errors += 1
                        logger.error("Failed player %d: %s", pid, e)

            logger.info("Player histories: %d fetched, %d errors", fetched, errors)
        else:
            logger.info("All player histories already cached.")
    else:
        logger.info("Skipping player history (--skip-history).")

    logger.info("Done.")


if __name__ == "__main__":
    main()
