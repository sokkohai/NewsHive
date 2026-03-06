"""Command-line interface for newshive.

Invocation modes:
  newshive                   # run pipeline directly (default)
  newshive pipeline          # same, backward-compatible subcommand form
  python -m src pipeline         # alternative invocation

Exit codes:
  0 = Success
  1 = Execution error (pipeline, extraction, etc.)
  2 = Configuration error
  3 = State store error
  130 = Interrupted by user
"""

# Load environment variables from .env file at the very beginning
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # If python-dotenv is not installed, continue without it
    pass

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional

from .pipeline import Pipeline
from .config import Configuration, ConfigError, ConfigLoader

logger = logging.getLogger(__name__)

# Log retention policy: Keep only last and current pipeline logs
LOG_RETENTION_DAYS = 1


def _add_pipeline_args(p: argparse.ArgumentParser) -> None:
    """Add pipeline arguments to a parser or subparser."""
    p.add_argument(
        "--config",
        default="./config.yaml",
        help="Path to configuration file (default: ./config.yaml)",
    )
    p.add_argument(
        "--state-store",
        default="./data/state_store.json",
        help="Path to state store file (default: ./data/state_store.json)",
    )
    p.add_argument(
        "--results",
        default="./data/results.json",
        help="Path to results file (default: ./data/results.json)",
    )
    p.add_argument(
        "--logs",
        default="./newshive.log",
        help="Path to log file (default: ./newshive.log)",
    )
    p.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    p.add_argument(
        "--site",
        default=None,
        help="Optional: Process only a specific web source (by URL or name)",
    )


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="newshive",
        description="newshive - Specification-driven content discovery and enrichment pipeline",
    )

    # Pipeline args at top level so `newshive [options]` runs the pipeline directly
    _add_pipeline_args(parser)

    subparsers = parser.add_subparsers(dest="command", help="Subcommand (optional; default: pipeline)")

    # Pipeline subcommand - kept for backward compatibility
    pipeline_parser = subparsers.add_parser("pipeline", help="Run content discovery and enrichment pipeline")
    _add_pipeline_args(pipeline_parser)

    args = parser.parse_args()

    # Default to pipeline when no subcommand is given
    if not args.command:
        args.command = "pipeline"

    # Configure logging
    log_level_name = getattr(args, "log_level", "INFO")
    log_level = getattr(logging, log_level_name)
    
    # Get log file path if available
    log_file = getattr(args, "logs", "./newshive.log")

    # Remove existing handlers to prevent duplicates
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Configure both file and console logging
    # Use TimedRotatingFileHandler for daily rotation with 1-day backup
    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=1,  # Keep only last and current pipeline logs
        utc=False
    )
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    
    # Use UTF-8 for console output to handle emojis and special characters on Windows
    # (default Windows console encoding is cp1252 which can't encode Unicode symbols)
    import io as _io
    _utf8_stream = _io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
    ) if hasattr(sys.stdout, "buffer") else sys.stdout
    console_handler = logging.StreamHandler(stream=_utf8_stream)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Suppress noisy asyncio error logging from Playwright when Group Policy blocks browsers
    logging.getLogger("asyncio").setLevel(logging.CRITICAL)

    if args.command == "pipeline":
        # Clean up old logs before running pipeline
        cleanup_old_logs(log_file)
        sys.exit(_run_pipeline(args))


def cleanup_old_logs(log_file_path: str, retention_days: int = LOG_RETENTION_DAYS) -> None:
    """Delete rotated log files older than retention period.
    
    Args:
        log_file_path: Path to the main log file
        retention_days: Keep logs newer than this many days (default 1 - last and current logs)
    """
    try:
        log_path = Path(log_file_path)
        log_dir = log_path.parent
        
        # Get the base name for finding rotated logs
        log_name = log_path.name
        
        now = datetime.now()
        cutoff = now - timedelta(days=retention_days)
        
        # Find and delete rotated log files
        for log_file in log_dir.glob(f"{log_name}*"):
            # Skip the main log file itself
            if log_file == log_path:
                continue
            
            # Check modification time
            mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
            if mtime < cutoff:
                try:
                    log_file.unlink()
                    logger.info(f"Deleted old log file: {log_file}")
                except Exception as e:
                    logger.warning(f"Failed to delete old log file {log_file}: {e}")
    except Exception as e:
        logger.warning(f"Log cleanup failed: {e}")


def _run_pipeline(args: argparse.Namespace) -> int:
    """Run pipeline only.
    
    Returns:
        Exit code:
          0 = Success
          1 = Execution error
          2 = Configuration error
          3 = State store error
    """
    print("=" * 70)
    print("newshive - RUNNING PIPELINE")
    print("=" * 70)
    print()

    try:
        # Validate config file exists and is loadable
        config_path = Path(args.config)
        if not config_path.exists():
            print(f"ERROR: Configuration file not found: {args.config}", file=sys.stderr)
            return 2

        try:
            config = ConfigLoader.load(config_path)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"ERROR: Invalid config file: {e}", file=sys.stderr)
            return 2
        except ConfigError as e:
            print(f"ERROR: Configuration error: {e}", file=sys.stderr)
            return 2

        # Run pipeline
        logger.info("PIPELINE EXECUTION STARTED")
        pipeline = Pipeline(
            state_store_path=args.state_store,
            results_path=args.results,
            site_filter=getattr(args, "site", None)
        )
        envelope = pipeline.run()

        print()
        print("=" * 70)
        print(f"PIPELINE COMPLETE")
        print(f"  Items processed: {len(envelope.items)}")
        print(f"  Items failed:    {len(envelope.failed_items)}")
        print("=" * 70)
        logger.info(
            f"Pipeline complete: {len(envelope.items)} items, {len(envelope.failed_items)} failed"
        )

        # Flush all handlers to ensure logs are written to disk
        for handler in logging.root.handlers:
            handler.flush()

        return 0

    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
        logger.warning("Pipeline interrupted by user")
        # Flush handlers on interruption
        for handler in logging.root.handlers:
            handler.flush()
        return 130  # SIGINT

    except FileNotFoundError as e:
        print(f"ERROR: File not found: {e}", file=sys.stderr)
        logger.exception("File not found during pipeline execution")
        # Flush handlers on error
        for handler in logging.root.handlers:
            handler.flush()
        return 3

    except json.JSONDecodeError as e:
        print(f"ERROR: State store file corrupted or unreadable: {e}", file=sys.stderr)
        logger.exception("State store file corrupted")
        # Flush handlers on error
        for handler in logging.root.handlers:
            handler.flush()
        return 3

    except ConfigError as e:
        print(f"ERROR: Configuration error: {e}", file=sys.stderr)
        logger.exception("Configuration error")
        # Flush handlers on error
        for handler in logging.root.handlers:
            handler.flush()
        return 2

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        logger.exception("Pipeline execution failed")
        # Flush handlers on error
        for handler in logging.root.handlers:
            handler.flush()
        return 1


if __name__ == "__main__":
    main()
