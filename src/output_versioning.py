"""Result file versioning and archive management.

Implements the timestamped results versioning system as specified in
specs/core/OUTPUT.md.

Features:
- Generate ISO 8601 timestamped filenames
- Write results to versioned archives
- Cleanup old files (30-day retention)
- List available result files for inspection
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class ResultsVersioning:
    """Manages timestamped result file versioning and cleanup."""

    ARCHIVE_DIR = "./data/results_archive"
    RETENTION_DAYS = 30
    FILE_PREFIX = "results_"
    FILE_SUFFIX = ".json"

    @staticmethod
    def get_timestamp_filename(execution_timestamp: str | datetime | None = None) -> str:
        """Generate ISO 8601 timestamped filename.

        Args:
            execution_timestamp: ISO 8601 timestamp or datetime object.
                                If None, uses current UTC time.

        Returns:
            Filename like: results_2026-01-16T143025Z.json
        """
        if execution_timestamp is None:
            dt = datetime.now(timezone.utc)
        elif isinstance(execution_timestamp, str):
            # Parse ISO 8601 string, e.g., "2026-01-16T14:30:25Z"
            # Replace colon in time to create valid filename
            dt = datetime.fromisoformat(execution_timestamp.replace("Z", "+00:00"))
        else:
            dt = execution_timestamp

        # Format as ISO 8601 with compact time (no colons): YYYYMMDDTHHHMMSSZ
        iso_str = dt.strftime("%Y-%m-%dT%H%M%SZ")
        return f"{ResultsVersioning.FILE_PREFIX}{iso_str}{ResultsVersioning.FILE_SUFFIX}"

    @staticmethod
    def get_archive_path(filename: str | None = None) -> Path:
        """Get full path to archive directory or specific file.

        Args:
            filename: Optional filename. If provided, returns full path to file.
                     If None, returns archive directory path.

        Returns:
            Path object for archive directory or file.
        """
        archive_path = Path(ResultsVersioning.ARCHIVE_DIR)
        if filename:
            return archive_path / filename
        return archive_path

    @staticmethod
    def write_results(items: list[dict[str, Any]], execution_timestamp: str) -> Path | None:
        """Write results to timestamped file in archive.

        Per specs/core/OUTPUT.md:
        - Creates archive directory if needed
        - Writes to results_TIMESTAMP.json
        - Returns path on success, None on failure
        - Logs errors but does not raise

        Args:
            items: List of ContentItem dicts to write
            execution_timestamp: ISO 8601 timestamp string

        Returns:
            Path to written file on success, None on failure
        """
        try:
            # Create archive directory
            archive_dir = ResultsVersioning.get_archive_path()
            archive_dir.mkdir(parents=True, exist_ok=True)

            # Generate timestamped filename
            filename = ResultsVersioning.get_timestamp_filename(execution_timestamp)
            filepath = archive_dir / filename

            # Prepare output document
            output_doc = {
                "execution_timestamp": execution_timestamp,
                "item_count": len(items),
                "items": items,
            }

            # Write to disk
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(output_doc, f, indent=2, ensure_ascii=False)

            logger.info(f"  Wrote {len(items)} items to {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"  Failed to write results file: {e}")
            return None

    @staticmethod
    def cleanup_old_results(retention_days: int = RETENTION_DAYS) -> int:
        """Delete result files older than retention period.

        Per specs/core/OUTPUT.md:
        - Scans archive directory
        - Deletes files older than retention_days
        - Logs deleted files
        - Continues on individual file delete failures

        Args:
            retention_days: Keep files newer than this many days (default 30)

        Returns:
            Number of files deleted
        """
        try:
            archive_dir = ResultsVersioning.get_archive_path()

            if not archive_dir.exists():
                logger.debug(f"Archive directory does not exist: {archive_dir}")
                return 0

            # Calculate cutoff time
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(days=retention_days)

            deleted_count = 0
            deleted_files = []

            # Scan for result files
            for file_path in archive_dir.glob(f"{ResultsVersioning.FILE_PREFIX}*{ResultsVersioning.FILE_SUFFIX}"):
                try:
                    # Extract timestamp from filename
                    # Format: results_2026-01-16T143025Z.json
                    filename = file_path.name
                    timestamp_str = filename[
                        len(ResultsVersioning.FILE_PREFIX) : -len(ResultsVersioning.FILE_SUFFIX)
                    ]

                    # Parse timestamp (e.g., "2026-01-16T143025Z")
                    file_datetime = datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M%SZ")
                    file_datetime = file_datetime.replace(tzinfo=timezone.utc)

                    # Check if file is older than retention period
                    if file_datetime < cutoff:
                        file_path.unlink()
                        deleted_count += 1
                        deleted_files.append(filename)
                        logger.debug(f"  Deleted old result file: {filename}")

                except Exception as e:
                    logger.warning(
                        f"  Failed to delete old result file {file_path.name}: {e}"
                    )
                    continue

            if deleted_count > 0:
                logger.info(
                    f"  Cleaned up {deleted_count} old result files "
                    f"(before {cutoff.isoformat()})"
                )

            return deleted_count

        except Exception as e:
            logger.warning(f"  Cleanup of old results failed: {e}")
            return 0

    @staticmethod
    def list_available_results(limit: int = 100) -> list[dict[str, Any]]:
        """List available result files for inspection.

        Returns list of dicts with metadata for each file, sorted by timestamp
        (newest first).

        Args:
            limit: Maximum number of files to return (default 100)

        Returns:
            List of dicts:
            [
                {
                    "filename": "results_2026-01-16T143025Z.json",
                    "timestamp": "2026-01-16T14:30:25Z",
                    "datetime": datetime object,
                    "item_count": 42,
                    "size_bytes": 15234,
                },
                ...
            ]
            Returns empty list if archive doesn't exist or no files found.
        """
        results = []

        try:
            archive_dir = ResultsVersioning.get_archive_path()

            if not archive_dir.exists():
                logger.debug(f"Archive directory does not exist: {archive_dir}")
                return []

            # Scan for result files
            files = list(
                archive_dir.glob(f"{ResultsVersioning.FILE_PREFIX}*{ResultsVersioning.FILE_SUFFIX}")
            )

            for file_path in files:
                try:
                    filename = file_path.name
                    timestamp_str = filename[
                        len(ResultsVersioning.FILE_PREFIX) : -len(ResultsVersioning.FILE_SUFFIX)
                    ]

                    # Parse timestamp
                    file_datetime = datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M%SZ")
                    file_datetime = file_datetime.replace(tzinfo=timezone.utc)

                    # Try to read item count from file
                    item_count = 0
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            item_count = data.get("item_count", 0)
                    except Exception:
                        pass

                    results.append(
                        {
                            "filename": filename,
                            "timestamp": file_datetime.isoformat().replace("+00:00", "Z"),
                            "datetime": file_datetime,
                            "item_count": item_count,
                            "size_bytes": file_path.stat().st_size,
                        }
                    )

                except Exception as e:
                    logger.debug(f"Failed to parse metadata for {file_path.name}: {e}")
                    continue

            # Sort by datetime (newest first)
            results.sort(key=lambda x: x["datetime"], reverse=True)

            # Apply limit
            results = results[:limit]

            return results

        except Exception as e:
            logger.error(f"Failed to list available results: {e}")
            return []

    @staticmethod
    def load_results_from_file(filename: str) -> dict[str, Any] | None:
        """Load and parse a specific result file.

        Args:
            filename: Name of the file to load (e.g., "results_2026-01-16T143025Z.json")

        Returns:
            Parsed JSON object with execution_timestamp, item_count, items
            Returns None on error
        """
        try:
            filepath = ResultsVersioning.get_archive_path(filename)

            if not filepath.exists():
                logger.warning(f"Result file not found: {filepath}")
                return None

            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data

        except Exception as e:
            logger.error(f"Failed to load results from {filename}: {e}")
            return None

    @staticmethod
    def get_latest_results() -> dict[str, Any] | None:
        """Load the most recent result file.

        Returns:
            Parsed JSON object with execution_timestamp, item_count, items
            Returns None if no result files exist
        """
        results = ResultsVersioning.list_available_results(limit=1)

        if not results:
            return None

        latest = results[0]
        return ResultsVersioning.load_results_from_file(latest["filename"])
