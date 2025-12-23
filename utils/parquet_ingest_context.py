import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add ingest context columns to a Parquet dataset."
    )
    parser.add_argument("--input", required=True, help="Input Parquet file or folder.")
    parser.add_argument("--output", required=True, help="Output Parquet file or folder.")
    parser.add_argument("--site-id", required=True, help="Site identifier.")
    parser.add_argument("--ingest-run-id", required=True, help="Ingest run identifier.")
    parser.add_argument(
        "--engine",
        choices=["spark", "pyarrow"],
        default="spark",
        help="Execution engine: spark (default) or pyarrow for local write.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250_000,
        help="Rows per batch when using pyarrow.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output.")
    return parser.parse_args()


def add_context_spark(df: DataFrame, site_id: str, ingest_run_id: str) -> DataFrame:
    return (
        df.withColumn("siteId", F.lit(site_id))
        .withColumn("ingestRunId", F.lit(ingest_run_id))
        .withColumn("ingestTime", F.current_timestamp())
    )


def run_spark(args: argparse.Namespace) -> int:
    spark = (
        SparkSession.builder.appName("parquet-ingest-context")
        .config("spark.sql.repl.eagerEval.enabled", "false")
        .getOrCreate()
    )

    df = spark.read.parquet(args.input)
    df_raw = add_context_spark(df, args.site_id, args.ingest_run_id)

    total_rows = df_raw.count()
    print(f"(writing {total_rows:,} rows)")

    mode = "overwrite" if args.overwrite else "error"
    df_raw.write.mode(mode).parquet(args.output)
    print(f"Wrote Parquet: {args.output}")
    return 0


def run_pyarrow(args: argparse.Namespace) -> int:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("pyarrow is required for --engine pyarrow.", file=sys.stderr)
        return 2

    input_path = Path(args.input)
    output_path = Path(args.output)

    if output_path.exists() and not args.overwrite:
        print(f"Output exists: {output_path} (use --overwrite)", file=sys.stderr)
        return 2

    parquet_file = pq.ParquetFile(str(input_path))
    total_rows = parquet_file.metadata.num_rows
    processed = 0

    writer = None
    try:
        for batch in parquet_file.iter_batches(batch_size=args.batch_size):
            ingest_time = datetime.now(timezone.utc)
            batch = batch.append_column("siteId", pa.array([args.site_id] * len(batch)))
            batch = batch.append_column(
                "ingestRunId", pa.array([args.ingest_run_id] * len(batch))
            )
            batch = batch.append_column(
                "ingestTime",
                pa.array([ingest_time] * len(batch), type=pa.timestamp("us", tz="UTC")),
            )

            table = pa.Table.from_batches([batch])
            if writer is None:
                writer = pq.ParquetWriter(
                    where=str(output_path),
                    schema=table.schema,
                    compression="snappy",
                )
            writer.write_table(table)

            processed += len(batch)
            if total_rows:
                pct = processed / total_rows * 100
                print(
                    f"\rProgress: {processed:,}/{total_rows:,} ({pct:.1f}%)",
                    end="",
                    file=sys.stderr,
                )
    finally:
        if writer is not None:
            writer.close()

    if total_rows:
        print(file=sys.stderr)
    print(f"Wrote Parquet: {output_path}")
    return 0


def main() -> int:
    args = parse_args()

    if args.batch_size <= 0:
        print("--batch-size must be > 0", file=sys.stderr)
        return 2

    if args.engine == "pyarrow":
        return run_pyarrow(args)

    return run_spark(args)


if __name__ == "__main__":
    raise SystemExit(main())
