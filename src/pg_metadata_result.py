from __future__ import annotations
import csv
import logging
from datetime import datetime
from pathlib import Path
from config.pg_config import PgConfig

logger = logging.getLogger(__name__)


class PgMetadataResult:
    def __init__(self, metadata_filepath: str, checksum: str, pg_config: PgConfig):
        self.metadata_filepath = metadata_filepath
        self.checksum = checksum
        self.pg_config = pg_config

    @staticmethod
    def _db_label(result: "PgMetadataResult", fallback_index: int) -> str:
        """Derive a concise label for the database using the exported filename."""
        return f"{result.pg_config.database}-{fallback_index}"

    @staticmethod
    def _write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
        with path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        logger.info("Wrote table to %s", path)

    @staticmethod
    def _format_table(headers: list[str], rows: list[list[str]]) -> str:
        """Return a simple monospace-friendly table string for console output."""
        col_widths = [len(str(h)) for h in headers]
        for row in rows:
            for idx, cell in enumerate(row):
                col_widths[idx] = max(col_widths[idx], len(str(cell)))

        def format_row(row: list[str]) -> str:
            return " | ".join(str(cell).ljust(col_widths[idx]) for idx, cell in enumerate(row))

        separator = "-+-".join("-" * width for width in col_widths)
        formatted_rows = [format_row(headers), separator]
        formatted_rows.extend(format_row(row) for row in rows)
        return "\n".join(formatted_rows)

    @staticmethod
    def output_tabulation_table(results: list["PgMetadataResult"], csv_folderpath: str, timestamp: str) -> None:
        if not results:
            logger.warning("No results provided; skipping tabulation output")
            return

        csv_dir = Path(csv_folderpath)
        csv_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Preparing tabulation tables in %s", csv_dir)

        labels = [PgMetadataResult._db_label(result, idx) for idx, result in enumerate(results)]
        checksums = [result.checksum for result in results]

        # Build checksum table with database labels and their checksums
        checksum_rows = [[label, checksum] for label, checksum in zip(labels, checksums)]

        # Build match/mismatch table comparing every pair of databases
        match_rows = []
        for row_idx, label in enumerate(labels):
            row = []
            for col_idx in range(len(labels)):
                row.append("MATCH" if checksums[row_idx] == checksums[col_idx] else "MISMATCH")
            match_rows.append([label] + row)

        headers = [""] + labels

        checksum_csv = csv_dir / f"{timestamp}-checksums.csv"
        match_csv = csv_dir / f"{timestamp}-checksum_match_matrix.csv"

        PgMetadataResult._write_csv(checksum_csv, ["Database", "Checksum"], checksum_rows)
        PgMetadataResult._write_csv(match_csv, headers, match_rows)

        print("\nChecksum:")
        print(PgMetadataResult._format_table(["Database", "Checksum"], checksum_rows))
        print("\nMatch/Mismatch Matrix:")
        print(PgMetadataResult._format_table(headers, match_rows))

        logger.info("Checksum and match tables printed to console and saved to CSV")
