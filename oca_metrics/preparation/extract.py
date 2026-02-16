from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

import datetime
import duckdb
import gzip
import logging
import multiprocessing
import orjson
import pandas as pd
import pathlib


logger = logging.getLogger(__name__)


def process_chunk(lines, start_year=2018, end_year=None):
    if end_year is None:
        end_year = datetime.datetime.now().year

    batch_results = []
    for line in lines:
        try:
            src = orjson.loads(line)
            
            # Remove documents that are not articles or are XPAC
            if src.get("type") != "article" or src.get("is_xpac") is True:
                continue

            # Remove documents that are not in the specified year range
            pub_year = src.get("publication_year")
            if not (pub_year and start_year <= pub_year <= end_year):
                continue

            # Collects journal information
            source = None
            p_loc = src.get("primary_location") or {}
            p_src = p_loc.get("source") or {}
            if p_src.get("type") == "journal":
                source = p_src
            else:
                for loc in src.get("locations", []):
                    if loc and loc.get("source") and loc["source"].get("type") == "journal":
                        source = loc["source"]
                        break

            if not source:
                continue

            pt = src.get("primary_topic") or {}

            res = {
                "work_id": src.get("id"),
                "publication_year": pub_year,
                "language": src.get("language"),
                "doi": src.get("doi"),
                "source_id": source.get("id"),
                "source_issn_l": source.get("issn_l"),
                "domain": pt.get("domain", {}).get("display_name"),
                "field": pt.get("field", {}).get("display_name"),
                "subfield": pt.get("subfield", {}).get("display_name"),
                "topic": pt.get("display_name"),
                "topic_score": pt.get("score"),
                "citations_total": src.get("cited_by_count", 0)
            }

            # Citations
            w2, w3, w5 = 0, 0, 0
            counts = src.get("counts_by_year", [])
            
            if isinstance(counts, list):
                for cy in counts:
                    y = cy.get("year")
                    tot = cy.get("cited_by_count", 0)
                    if not y:
                        continue
                    
                    res[f"citations_{y}"] = tot

                    if y > pub_year:
                        if y <= pub_year + 2: w2 += tot
                        if y <= pub_year + 3: w3 += tot
                        if y <= pub_year + 5: w5 += tot

            res.update({
                "citations_window_2y": w2,
                "citations_window_3y": w3,
                "citations_window_5y": w5,
                "has_citation_window_2y": 1 if w2 > 0 else 0,
                "has_citation_window_3y": 1 if w3 > 0 else 0,
                "has_citation_window_5y": 1 if w5 > 0 else 0
            })
            
            batch_results.append(res)
        except:
            continue

    return batch_results

def load_processed_ids(output_dir):
    parquet_files = list(pathlib.Path(output_dir).glob("metrics_*.parquet"))
    if not parquet_files:
        return set()
    
    logger.info(f"Retrieving IDs from {len(parquet_files)} existing files...")

    con = duckdb.connect()
    ids = con.execute(f"SELECT work_id FROM read_parquet('{output_dir}/*.parquet')").fetchall()

    return set(i[0] for i in ids)

def run_extraction(base_dir, output_dir, start_year=2018, end_year=None, batch_size=500_000, num_cores=None):
    if end_year is None:
        end_year = datetime.datetime.now().year

    base_dir = pathlib.Path(base_dir)
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    seen_ids = load_processed_ids(output_dir)
    
    folders = sorted(base_dir.glob("updated_date=*"), reverse=True)
    if num_cores is None:
        num_cores = max(1, multiprocessing.cpu_count() - 2)
    
    logger.info(f"Starting extraction | {num_cores} cores | {len(seen_ids)} known IDs")

    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        for folder in folders:
            date_str = folder.name.split("=")[1]

            existing = list(output_dir.glob(f"metrics_{date_str}*.parquet"))
            if existing:
                logger.info(f"Date {date_str} already has files. Skipping...")
                continue
            
            files = sorted(folder.glob("part_*.gz"))
            day_results = []
            part_counter = 0
            
            pbar = tqdm(files, desc=f"Processing {date_str}", unit="file")
            for f_path in pbar:
                with gzip.open(f_path, "rb") as f:
                    lines = f.readlines()
                
                if not lines:
                    continue
                
                chunk_size = max(1, len(lines) // num_cores)
                futures = [executor.submit(process_chunk, lines[i:i + chunk_size], start_year, end_year) 
                           for i in range(0, len(lines), chunk_size)]
                
                for future in futures:
                    for item in future.result():
                        if item["work_id"] not in seen_ids:
                            seen_ids.add(item["work_id"])
                            day_results.append(item)
                
                if len(day_results) >= batch_size:
                    df = pd.DataFrame(day_results)
                    output_file = output_dir / f"metrics_{date_str}_part_{part_counter}.parquet"
                    df.to_parquet(output_file, index=False, engine="pyarrow", compression="snappy")
                    
                    day_results = []
                    part_counter += 1
                    pbar.set_postfix({"status": f"Saved part_{part_counter-1}"})

            if day_results:
                df = pd.DataFrame(day_results)

                output_file = output_dir / f"metrics_{date_str}_part_{part_counter}.parquet"
                df.to_parquet(output_file, index=False, engine="pyarrow", compression="snappy")

            elif part_counter == 0:
                (output_dir / f"metrics_{date_str}_empty.parquet").touch()
