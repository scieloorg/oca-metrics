import unittest

from oca_metrics.preparation.scielo import merge_scielo_documents


class TestMergeStrategies(unittest.TestCase):
    def setUp(self):
        self.docs = [
            {
                "collection": "scl",
                "pid_v2": "PID1",
                "doi": "10.1590/1",
                "titles": ["A Very Long Title that exceeds fifteen characters"],
                "publication_year": 2024,
                "journal_issns": ["1234-5678"],
                "journal_title": "Journal A"
            },
            {
                "collection": "scl",
                "pid_v2": "PID2",
                "doi": "10.1590/1", # Same DOI
                "titles": ["A Very Long Title that exceeds fifteen characters"], # Same Title
                "publication_year": 2024,
                "journal_issns": ["1234-5678"],
                "journal_title": "Journal A"
            },
            {
                "collection": "scl",
                "pid_v2": "PID3",
                "doi": "10.1590/3",
                "titles": ["Title Three"],
                "publication_year": 2024,
                "journal_issns": ["1111-2222"],
                "journal_title": "Journal B"
            }
        ]

    def test_merge_all_strategies(self):
        # Default should merge doc 0 and 1 because of DOI
        merged = merge_scielo_documents(self.docs)
        self.assertEqual(len(merged), 2)

    def test_no_strategies(self):
        # No strategies, no merge
        merged = merge_scielo_documents(self.docs, strategies=())
        self.assertEqual(len(merged), 3)

    def test_only_pid_strategy_no_match(self):
        # docs have different PIDs, so no merge with only pid strategy
        merged = merge_scielo_documents(self.docs, strategies=("pid",))
        self.assertEqual(len(merged), 3)

    def test_doi_strategy_only(self):
        # Should merge because of DOI
        merged = merge_scielo_documents(self.docs, strategies=("doi",))
        self.assertEqual(len(merged), 2)

    def test_title_strategy_only(self):
        # Should merge because of Title
        merged = merge_scielo_documents(self.docs, strategies=("title",))
        self.assertEqual(len(merged), 2)


if __name__ == "__main__":
    unittest.main()
