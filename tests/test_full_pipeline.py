from pathlib import Path

import json
import pandas as pd
import pytest
import shutil
import tempfile

from oca_metrics.preparation.extract import run_extraction
from oca_metrics.preparation.scielo import (
    load_bson_scl,
    merge_scielo_documents,
)
from oca_metrics.preparation.integration import (
    generate_merged_parquet,
    match_scielo_with_openalex,
)
from oca_metrics.adapters.parquet import ParquetAdapter
from oca_metrics.core import MetricsEngine


@pytest.fixture
def pipeline_env():
    base_dir = Path(__file__).parent.parent
    oa_fixture = base_dir / "tests/fixtures/openalex/sample.jsonl.gz"
    scielo_fixture = base_dir / "tests/fixtures/scielo/sample.bson"
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        
        oa_input_dir = tmp_dir_path / "oa_input"
        oa_output_dir = tmp_dir_path / "oa_parquet"
        
        updated_date_dir = oa_input_dir / "updated_date=2024-01-01"
        updated_date_dir.mkdir(parents=True)
        shutil.copy(oa_fixture, updated_date_dir / "part_000.gz")
        
        yield {
            "tmp_dir": tmp_dir_path,
            "oa_input_dir": oa_input_dir,
            "oa_output_dir": oa_output_dir,
            "scielo_fixture": scielo_fixture,
            "start_year": 2010,
            "end_year": 2025
        }


def test_full_pipeline_with_fixtures(pipeline_env):
    tmp_dir = pipeline_env["tmp_dir"]
    oa_input_dir = pipeline_env["oa_input_dir"]
    oa_output_dir = pipeline_env["oa_output_dir"]
    scielo_fixture = pipeline_env["scielo_fixture"]
    start_year = pipeline_env["start_year"]
    end_year = pipeline_env["end_year"]
    
    # 1. Step: OpenAlex Extraction
    run_extraction(
        base_dir=oa_input_dir,
        output_dir=oa_output_dir,
        start_year=start_year,
        end_year=end_year,
        num_cores=1
    )
    
    oa_parquet_files = list(oa_output_dir.glob("*.parquet"))
    assert len(oa_parquet_files) > 0, "OpenAlex extraction failed to produce Parquet files"
    
    # 2. Step: SciELO Preparation
    scielo_output_jsonl = tmp_dir / "scielo_merged.jsonl"
    
    scl_docs = load_bson_scl(scielo_fixture, start_year=start_year, end_year=end_year)
    assert len(scl_docs) > 0, "Failed to load SciELO BSON fixture"
    
    merged_scl = merge_scielo_documents(scl_docs)
    assert len(merged_scl) > 0, "SciELO merging resulted in zero documents"
    
    with open(scielo_output_jsonl, "w") as f:
        for doc in merged_scl:
            f.write(json.dumps(doc) + "\n")
            
    # 3. Step: Integration
    final_parquet = tmp_dir / "merged_data.parquet"
    
    scl_oa_merged, unified_schema = match_scielo_with_openalex(
        merged_scl,
        str(oa_output_dir),
        start_year=start_year,
        end_year=end_year
    )
    
    generate_merged_parquet(
        scl_oa_merged,
        str(oa_output_dir),
        str(final_parquet),
        unified_schema
    )
    
    assert final_parquet.exists(), "Final merged Parquet file was not created"

    df = pd.read_parquet(final_parquet)
    assert len(df) > 0, "Final merged Parquet is empty"
    
    matches = df[df["scielo_pid_v2"].notna() & (df["scielo_pid_v2"].apply(len) > 0)]
    
    print("\n" + "="*50)
    print("INTEGRATION RESULTS (Sample)")
    print("="*50)
    print(f"Total records in final Parquet: {len(df)}")
    print(f"Total records with SciELO link: {len(matches)}")

    if not matches.empty:
        sample_size = min(5, len(matches))
        print(f"\nSample of {sample_size} merged documents (SciELO + OpenAlex):")
        display_cols = ["work_id", "doi", "publication_year", "scielo_pid_v2"]

        available_cols = [c for c in display_cols if c in matches.columns]
        print(matches[available_cols].head(sample_size).to_string(index=False))
    
    assert len(matches) > 0, "No SciELO-OpenAlex matches found in fixture data"
    
    # 4. Step: Metrics Computation
    adapter = ParquetAdapter(str(final_parquet))
    engine = MetricsEngine(adapter)
    
    query = f"SELECT publication_year, field FROM {adapter.table_name} WHERE field IS NOT NULL LIMIT 1"
    res = adapter.con.execute(query).fetchall()
    if res:
        best_year, cat_id = res[0]
        best_year = int(best_year)
        level = "field"
        
        df_metrics = engine.process_category(best_year, level, cat_id, windows=[2, 3, 5])
        
        assert df_metrics is not None
        assert not df_metrics.empty
        
        print("\n" + "="*50)
        print(f"METRICS RESULTS (Year: {best_year}, Category: {cat_id})")
        print("="*50)
        
        metric_cols = [
            "journal_title", 
            "journal_publications_count", 
            "journal_citations_total", 
            "journal_impact_normalized",
            "top_10pct_all_time_publications_count"
        ]
        available_metrics = [c for c in metric_cols if c in df_metrics.columns]
        print(df_metrics[available_metrics].head(10).to_string(index=False))
        
        assert "journal_impact_normalized" in df_metrics.columns
        assert "top_5pct_all_time_publications_count" in df_metrics.columns
        assert "top_10pct_all_time_publications_count" in df_metrics.columns
