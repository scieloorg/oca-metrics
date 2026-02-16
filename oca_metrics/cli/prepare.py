import argparse
import datetime
import json
import logging
import sys

from oca_metrics.preparation.extract import run_extraction
from oca_metrics.preparation.integration import (
    generate_merged_parquet,
    match_scielo_with_openalex,
)
from oca_metrics.preparation.scielo import (
    load_bson_scl,
    load_raw_scl,
    merge_scielo_documents,
)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Data preparation tools for oca-metrics.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: extract-oa
    parser_oa = subparsers.add_parser("extract-oa", help="Extract metrics from OpenAlex snapshots")
    parser_oa.add_argument("--base-dir", required=True, help="Base directory with .gz snapshots")
    parser_oa.add_argument("--output-dir", required=True, help="Output directory for Parquet files")
    parser_oa.add_argument("--start-year", type=int, default=2018)
    parser_oa.add_argument("--end-year", type=int, default=datetime.datetime.now().year)
    parser_oa.add_argument("--batch-size", type=int, default=500000)

    # Command: prepare-scielo
    parser_scl = subparsers.add_parser("prepare-scielo", help="Load and merge SciELO documents")
    parser_scl.add_argument("--input", required=True, help="Path to SciELO file (JSONL or BSON)")
    parser_scl.add_argument("--format", choices=["jsonl", "bson"], default="jsonl")
    parser_scl.add_argument("--output-jsonl", required=True, help="Path to save merged documents")
    parser_scl.add_argument("--start-year", type=int, default=2018)
    parser_scl.add_argument("--end-year", type=int, default=datetime.datetime.now().year)
    parser_scl.add_argument("--audit-log", help="Path to merge audit log")
    parser_scl.add_argument("--strategies", nargs="+", choices=["doi", "pid", "title"], default=["doi", "pid", "title"], help="Merge strategies to use")

    # Command: integrate
    parser_int = subparsers.add_parser("integrate", help="Cross SciELO with OpenAlex and generate merged Parquet")
    parser_int.add_argument("--scielo-jsonl", required=True, help="JSONL file of merged SciELO articles")
    parser_int.add_argument("--oa-parquet-dir", required=True, help="Directory with OpenAlex Parquet files")
    parser_int.add_argument("--output-parquet", required=True, help="Path for the final merged Parquet file")
    parser_int.add_argument("--start-year", type=int, default=2018)
    parser_int.add_argument("--end-year", type=int, default=datetime.datetime.now().year)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(2)

    if args.command == "extract-oa":
        run_extraction(
            base_dir=args.base_dir,
            output_dir=args.output_dir,
            start_year=args.start_year,
            end_year=args.end_year,
            batch_size=args.batch_size
        )

    elif args.command == "prepare-scielo":
        if args.format == "jsonl":
            docs = load_raw_scl(args.input, args.start_year, args.end_year)
        else:
            docs = load_bson_scl(args.input, args.start_year, args.end_year)
        
        merged = merge_scielo_documents(docs, audit_log_path=args.audit_log, strategies=tuple(args.strategies))
        
        logger.info(f"Saving {len(merged)} merged documents to {args.output_jsonl}")
        with open(args.output_jsonl, "w") as f:
            for doc in merged:
                f.write(json.dumps(doc) + "\n")
                
    elif args.command == "integrate":
        logger.info(f"Reading SciELO documents from {args.scielo_jsonl}")
        with open(args.scielo_jsonl, "r") as f:
            scl_docs = [json.loads(line) for line in f]
            
        scl_oa_merged, unified_schema = match_scielo_with_openalex(
            scl_docs, 
            args.oa_parquet_dir,
            start_year=args.start_year,
            end_year=args.end_year
        )
        
        generate_merged_parquet(
            scl_oa_merged,
            args.oa_parquet_dir,
            args.output_parquet,
            unified_schema
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
