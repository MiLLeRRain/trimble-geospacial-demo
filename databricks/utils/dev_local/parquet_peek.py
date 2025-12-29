import argparse
import sys

from pyspark.sql import SparkSession


def parquet_peek(df, n: int = 5) -> None:
    """Print the first n rows of a DataFrame-like object."""
    print(f"(showing first {n} rows of {df.count()} total rows)")
    df.show(n)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print the first N rows of a Parquet file.")
    parser.add_argument(
        "--path",
        "--file",
        dest="path",
        required=True,
        help="Path to a Parquet file or folder.",
    )
    parser.add_argument("--n", type=int, default=5, help="Number of rows to show.")
    parser.add_argument(
        "--engine",
        choices=["spark", "pyarrow"],
        default="spark",
        help="Execution engine: spark (default) or pyarrow for local read.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.n <= 0:
        print("--n must be > 0", file=sys.stderr)
        return 2

    if args.engine == "pyarrow":
        try:
            import pyarrow.parquet as pq
        except ImportError:
            print("pyarrow is required for --engine pyarrow.", file=sys.stderr)
            return 2

        table = pq.read_table(args.path)
        print(f"(showing first {args.n} rows of {table.num_rows} total rows)")
        preview = table.slice(0, args.n).to_pandas()
        print(preview.to_string(index=False))
        return 0

    spark = (
        SparkSession.builder.appName("parquet-peek")
        .config("spark.sql.repl.eagerEval.enabled", "false")
        .getOrCreate()
    )
    df = spark.read.parquet(args.path)
    parquet_peek(df, n=args.n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
