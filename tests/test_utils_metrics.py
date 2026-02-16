import unittest
import pandas as pd
import os

from oca_metrics.utils.metrics import (
    compute_normalized_impact,
    compute_percentiles,
    format_output_header_name,
    get_csv_schema_order,
    load_global_metadata,
    shorten_openalex_id
)


class TestUtilsMetrics(unittest.TestCase):

    def test_shorten_openalex_id(self):
        self.assertEqual(shorten_openalex_id("https://openalex.org/S123"), "S123")
        self.assertEqual(shorten_openalex_id("S123"), "S123")
        self.assertEqual(shorten_openalex_id(123), 123)
        self.assertEqual(shorten_openalex_id(None), None)

    def test_format_output_header_name(self):
        self.assertEqual(format_output_header_name("journal_id"), "journal id")
        self.assertEqual(format_output_header_name("top_1pct_share"), "top 1pct share")

    def test_get_csv_schema_order(self):
        windows = [2, 3]
        percentiles = [99, 50]
        schema = get_csv_schema_order(windows, percentiles)
        
        expected_start = ["category_id", "topic_level", "journal_id"]
        for col in expected_start:
            self.assertIn(col, schema)
            
        self.assertIn("category_citations_mean_window_2y", schema)
        self.assertIn("top_1pct_all_time_citations_threshold", schema)
        self.assertIn("top_50pct_window_3y_publications_share_pct", schema)

    def test_load_global_metadata(self):
        # Create a dummy excel file
        filename = "test_meta.xlsx"
        df = pd.DataFrame({
            'OpenAlex ID': ['S1', 'S2'],
            'Journal': ['J1', 'J2'],
            'YEAR': [2024, 2024],
            'is SciELO': [1, 0],
            'SciELO Active and Valid in the Year': [1, 0],
            'SciELO network country': ['Brazil', '']
        })
        df.to_excel(filename, index=False)
        
        try:
            loaded_df = load_global_metadata(filename)
            self.assertFalse(loaded_df.empty)
            self.assertIn('source_id', loaded_df.columns)
            self.assertIn('journal_title', loaded_df.columns)
            self.assertEqual(loaded_df.iloc[0]['source_id'], "https://openalex.org/S1")
            self.assertEqual(loaded_df.iloc[0]['is_scielo'], 1)
        finally:
            if os.path.exists(filename):
                os.remove(filename)

    def test_load_global_metadata_not_found(self):
        df = load_global_metadata("non_existent.xlsx")
        self.assertTrue(df.empty)

    def test_compute_normalized_impact(self):
        self.assertEqual(compute_normalized_impact(10, 5), 2.0)
        self.assertEqual(compute_normalized_impact(0, 5), 0.0)
        self.assertEqual(compute_normalized_impact(10, 0), 0.0)

    def test_compute_percentiles(self):
        citations = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        # median is 50
        res = compute_percentiles(citations, [0.5])
        self.assertEqual(res[0.5], 50.0)
        
        # empty
        res = compute_percentiles([], [0.5])
        self.assertEqual(res[0.5], 0.0)

    def test_compute_percentiles_invalid_input(self):
        with self.assertRaises(ValueError):
            compute_percentiles(None, [0.5])
        with self.assertRaises(ValueError):
            compute_percentiles('notalist', [0.5])
        with self.assertRaises(ValueError):
            compute_percentiles([1, 2, 3], None)


if __name__ == '__main__':
    unittest.main()
