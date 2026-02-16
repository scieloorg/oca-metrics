import pandas as pd
import pathlib
import unittest

from oca_metrics.preparation.integration import (
    match_scielo_with_openalex,
    generate_merged_parquet,
)


class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = pathlib.Path("tmp_test_integration")
        self.tmp_dir.mkdir(exist_ok=True)
        self.oa_parquet_dir = self.tmp_dir / "oa_parquet"
        self.oa_parquet_dir.mkdir(exist_ok=True)
        self.output_parquet = self.tmp_dir / "merged.parquet"

        # Fictitious OpenAlex data
        oa_data = {
            'work_id': ['https://openalex.org/W1', 'https://openalex.org/W2', 'https://openalex.org/W3'],
            'doi': ['https://doi.org/10.1001/1', 'https://doi.org/10.1001/2', None],
            'publication_year': [2024, 2024, 2024],
            'language': ['en', 'pt', 'en'],
            'source_id': ['S1', 'S1', 'S2'],
            'domain': ['Health', 'Health', 'Social'],
            'field': ['Medicine', 'Medicine', 'Sociology'],
            'subfield': ['Surgery', 'Surgery', 'Theory'],
            'topic': ['Surgery', 'Surgery', 'Theory'],
            'citations_total': [10, 5, 2],
            'citations_window_2y': [2, 1, 0],
            'citations_window_3y': [3, 2, 0],
            'citations_window_5y': [5, 3, 0],
            'citations_2024': [1, 1, 0]
        }
        df_oa = pd.DataFrame(oa_data)
        df_oa.to_parquet(self.oa_parquet_dir / "oa.parquet")
        
        # Fictitious SciELO data
        # Article 1: Matches W1 and W2 (multilingual)
        # Article 2: No match
        self.scl_docs = [
            {
                'collection': ['scl'],
                'pid_v2': ['S0001'],
                'doi': '10.1001/1',
                'doi_with_lang': {'en': '10.1001/1', 'pt': '10.1001/2'},
                'publication_year': 2024,
                'titles': ['Title 1', 'Titulo 1']
            },
            {
                'collection': ['scl'],
                'pid_v2': ['S0002'],
                'doi': '10.1001/999',
                'doi_with_lang': {},
                'publication_year': 2024,
                'titles': ['Title 2']
            }
        ]

    def tearDown(self):
        import shutil
        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir)

    def test_full_integration_flow(self):
        # 1. Test match_scielo_with_openalex
        scl_oa_merged, unified_schema = match_scielo_with_openalex(
            self.scl_docs, 
            str(self.oa_parquet_dir),
            start_year=2020
        )
        
        self.assertEqual(len(scl_oa_merged), 2)

        # Article 1 should have a match
        self.assertTrue(scl_oa_merged[0]['has_oa_match'])
        self.assertEqual(len(scl_oa_merged[0]['oa_metrics']['work_ids']), 2)

        # Global totals: W1(10) + W2(5) = 15
        self.assertEqual(scl_oa_merged[0]['oa_metrics']['global_totals']['total_citations'], 15)
        
        # Article 2 should not have a match
        self.assertFalse(scl_oa_merged[1]['has_oa_match'])

        # 2. Test generate_merged_parquet
        generate_merged_parquet(
            scl_oa_merged,
            str(self.oa_parquet_dir),
            str(self.output_parquet),
            unified_schema
        )
        
        self.assertTrue(self.output_parquet.exists())
        df_final = pd.read_parquet(self.output_parquet)
        
        # Expected:
        # - 1 merged record for S0001 (W1 and W2 consolidated)
        # - 1 record for W3 (OpenAlex without SciELO)
        # - 1 record for S0002 (SciELO without OpenAlex)
        self.assertEqual(len(df_final), 3)
        
        # Check merged record
        merged_row = df_final[df_final['is_merged'] == True].iloc[0]
        self.assertEqual(merged_row['citations_total'], 15)
        self.assertIn('https://openalex.org/W1', merged_row['all_work_ids'])
        self.assertIn('https://openalex.org/W2', merged_row['all_work_ids'])
        
        # Check SciELO record without match
        unmatched_scl = df_final[df_final['work_id'] == 'scielo:S0002'].iloc[0]
        self.assertEqual(unmatched_scl['citations_total'], 0)
        self.assertEqual(unmatched_scl['publication_year'], 2024)


if __name__ == '__main__':
    unittest.main()
