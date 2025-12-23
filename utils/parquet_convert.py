import argparse
import sys
from pathlib import Path

import laspy
import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_COLUMNS = [
    "x",
    "y",
    "z",
    "intensity",
    "classification",
    "return_number",
    "number_of_returns",
    "gps_time",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert LAS/LAZ to Parquet for raw storage."
    )
    parser.add_argument("input", help="Path to .las or .laz file")
    parser.add_argument("output", help="Path to output .parquet file")
    parser.add_argument(
        "--columns",
        help="Comma-separated list of columns to export",
    )
    parser.add_argument(
        "--keep-all",
        action="store_true",
        help="Export all LAS dimensions (plus x/y/z scaled).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500_000,
        help="Number of points per chunk",
    )
    parser.add_argument(
        "--compression",
        default="snappy",
        help="Parquet compression (snappy, zstd, gzip, none)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists",
    )
    return parser.parse_args()


def resolve_columns(requested, dims):
    dims_lower = {d.lower(): d for d in dims}
    resolved = []
    missing = []

    for col in requested:
        if col.lower() in ("x", "y", "z"):
            resolved.append(col.lower())
            continue
        if col in dims:
            resolved.append(col)
            continue
        if col.lower() in dims_lower:
            resolved.append(dims_lower[col.lower()])
            continue
        missing.append(col)

    # De-duplicate while preserving order.
    seen = set()
    unique = []
    for col in resolved:
        if col not in seen:
            unique.append(col)
            seen.add(col)
    return unique, missing


def extract_column(points, col):
    if col in ("x", "y", "z"):
        return getattr(points, col)
    return points[col]


def main() -> int:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"Input not found: {input_path}", file=sys.stderr)
        return 2

    if output_path.exists() and not args.overwrite:
        print(f"Output exists: {output_path} (use --overwrite)", file=sys.stderr)
        return 2

    with laspy.open(str(input_path)) as reader:
        dims = list(reader.header.point_format.dimension_names)

        if args.keep_all:
            requested = ["x", "y", "z"] + dims
        elif args.columns:
            requested = [c.strip() for c in args.columns.split(",") if c.strip()]
        else:
            requested = DEFAULT_COLUMNS

        columns, missing = resolve_columns(requested, dims)
        if missing:
            print(f"Warning: missing columns ignored: {', '.join(missing)}")

        if not columns:
            print("No valid columns selected.", file=sys.stderr)
            return 2

        total_points = reader.header.point_count
        processed = 0
        writer = None
        try:
            for chunk in reader.chunk_iterator(args.chunk_size):
                table = pa.table({col: extract_column(chunk, col) for col in columns})
                if writer is None:
                    writer = pq.ParquetWriter(
                        where=str(output_path),
                        schema=table.schema,
                        compression=None if args.compression == "none" else args.compression,
                    )
                writer.write_table(table)
                processed += len(chunk)
                if total_points:
                    pct = processed / total_points * 100
                    print(
                        f"\rProgress: {processed:,}/{total_points:,} ({pct:.1f}%)",
                        end="",
                        file=sys.stderr,
                    )
        finally:
            if writer is not None:
                writer.close()

    if total_points:
        print(file=sys.stderr)
    print(f"Wrote Parquet: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
