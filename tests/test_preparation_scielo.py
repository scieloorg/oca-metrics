from unittest.mock import MagicMock

import unittest

from oca_metrics.preparation.scielo import (
    extract_journal_issns,
    extract_titles,
    merge_scielo_documents,
    transform_article_to_doc,
)


class TestSciELOPreparation(unittest.TestCase):
    def setUp(self):
        # Mock Article object from xylose
        self.mock_article = MagicMock()
        self.mock_article.collection_acronym = "scl"
        self.mock_article.publisher_id = "S0101-01012024000100001"
        self.mock_article.doi = "10.1590/s0101-01012024000100001"
        self.mock_article.doi_and_lang = [["en", "10.1590/s0101-01012024000100001"]]
        self.mock_article.document_type = "research-article"

        # Mock Journal object from xylose
        mock_journal = MagicMock()
        mock_journal.title = "Revista de Teste"
        mock_journal.any_issn = "0101-0101"
        mock_journal.scielo_issn = "0101-0101"
        mock_journal.electronic_issn = "1111-2222"
        mock_journal.print_issn = "0101-0101"
        self.mock_article.journal = mock_journal
        
        self.mock_article.original_title.return_value = "Título Original"
        self.mock_article.translated_titles.return_value = {"en": "English Title", "es": "Título Español"}

    def test_extract_journal_issns_returns_expected_issns(self):
        issns = extract_journal_issns(self.mock_article)
        self.assertEqual(issns, ["0101-0101", "1111-2222"])

    def test_extract_titles_returns_all_titles(self):
        titles = extract_titles(self.mock_article)
        # stz_title removes spaces and accents and converts to lowercase
        self.assertIn("titulooriginal", titles)
        self.assertIn("englishtitle", titles)
        self.assertIn("tituloespanol", titles)

    def test_transform_article_to_doc_creates_expected_dict(self):
        doc = transform_article_to_doc(self.mock_article, 2024)
        self.assertEqual(doc["collection"], "scl")
        self.assertEqual(doc["pid_v2"], "S0101-01012024000100001")
        self.assertEqual(doc["doi"], "10.1590/s0101-01012024000100001")
        self.assertEqual(doc["publication_year"], 2024)
        self.assertIn("titulooriginal", doc["titles"])

    def test_merge_equal_doi_title(self):
        doc1 = {
            "collection": "scl",
            "pid_v2": "PID9876543210",
            "doi": "10.1590/1",
            "titles": ["titulounico"],
            "publication_year": 2024,
            "journal_issns": ["0101-0101"]
        }

        doc2 = {
            "collection": "spa",
            "pid_v2": "PID9876543210",
            "doi": "10.1590/1",
            "titles": ["titulounico"],
            "publication_year": 2025,
            "journal_issns": ["0101-0102"]
        }

        unique_docs = merge_scielo_documents([doc1, doc2])
        self.assertEqual(len(unique_docs), 1)

        self.assertIn("scl", unique_docs[0]["collection"])
        self.assertIn("spa", unique_docs[0]["collection"])
        self.assertEqual(unique_docs[0]["doi"], "10.1590/1")

    def test_no_merge_equal_doi_diff_title(self):
        doc1 = {
            "collection": "scl",
            "pid_v2": "PID0123456789",
            "doi": "10.1590/1",
            "titles": ["tituloum"],
            "publication_year": 2024,
            "journal_issns": ["0101-0101"]
        }

        doc2 = {
            "collection": "spa",
            "pid_v2": "PID9876543210",
            "doi": "10.1590/1",
            "titles": ["titulodois"],
            "publication_year": 2024,
            "journal_issns": ["0101-0101"]
        }

        unique_docs = merge_scielo_documents([doc1, doc2])

        self.assertEqual(len(unique_docs), 2)

    def test_no_merge_diff_doi_equal_title(self):
        doc1 = {
            "collection": "scl",
            "pid_v2": "PID0123456789",
            "doi": "10.1590/9999",
            "titles": ["titulo1"],
            "publication_year": 2024
        }

        doc2 = {
            "collection": "spa",
            "pid_v2": "PID9876543210",
            "doi": "10.1590/1000",
            "titles": ["titulo1"],
            "publication_year": 2024
        }

        unique_docs = merge_scielo_documents([doc1, doc2])

        self.assertEqual(len(unique_docs), 2)

    def test_merge_diff_doi_equal_pid_title_year_journal(self):
        doc1 = {
            "collection": "scl",
            "pid_v2": "PID0123456789",
            "doi": "10.1590/9999",
            "titles": ["titulo1"],
            "publication_year": 2024,
            "journal_title": "Journal A"
        }

        doc2 = {
            "collection": "spa",
            "pid_v2": "PID0123456789",
            "doi": "10.1590/1000",
            "titles": ["titulo1"],
            "publication_year": 2024,
            "journal_title": "Journal A"
        }

        unique_docs = merge_scielo_documents([doc1, doc2])

        self.assertEqual(len(unique_docs), 1)

    def test_no_merge_diff_doi_equal_pid_title_diff_year_journal(self):
        doc1 = {
            "collection": "scl",
            "pid_v2": "PID0123456789",
            "doi": "10.1590/9999",
            "titles": ["titulo1"],
            "publication_year": 2024,
            "journal_title": "Journal A"
        }

        doc2 = {
            "collection": "spa",
            "pid_v2": "PID0123456789",
            "doi": "10.1590/1000",
            "titles": ["titulo1"],
            "publication_year": 2025,
            "journal_title": "Journal A"
        }

        unique_docs = merge_scielo_documents([doc1, doc2])

        self.assertEqual(len(unique_docs), 2)

    def test_no_merge_diff_doi_equal_pid_title_year_diff_journal(self):
        doc1 = {
            "collection": "scl",
            "pid_v2": "PID0123456789",
            "doi": "10.1590/9999",
            "titles": ["titulo1"],
            "publication_year": 2024,
            "journal_title": "Journal A"
        }

        doc2 = {
            "collection": "spa",
            "pid_v2": "PID0123456789",
            "doi": "10.1590/1000",
            "titles": ["titulo1"],
            "publication_year": 2024,
            "journal_title": "Journal B"
        }

        unique_docs = merge_scielo_documents([doc1, doc2])

        self.assertEqual(len(unique_docs), 2)

    def test_merge_equal_doi_multi_title(self):
        doc1 = {
            "collection": "scl",
            "pid_v2": "PID0123456789",
            "doi": "10.1590/1",
            "titles": ["titulo1", "title1"],
            "publication_year": 2024
        }

        doc2 = {
            "collection": "scl",
            "pid_v2": "PID9876543210",
            "doi": "10.1590/1",
            "titles": ["title1", "tituloum"],
            "publication_year": 2024
        }

        unique_docs = merge_scielo_documents([doc1, doc2])

        self.assertEqual(len(unique_docs), 1)

    def test_no_merge_diff_doi_multi_title(self):
        doc1 = {
            "collection": "scl",
            "pid_v2": "PID0123456789",
            "doi": "10.1590/0000",
            "titles": ["titulo1", "title1"],
            "journal_title": "Journal A",
            "publication_year": 2024
        }

        doc2 = {
            "collection": "scl",
            "pid_v2": "PID9876543210",
            "doi": "10.1590/9999",
            "titles": ["title1", "tituloum"],
            "journal_title": "Journal A",
            "publication_year": 2024
        }

        unique_docs = merge_scielo_documents([doc1, doc2])

        self.assertEqual(len(unique_docs), 2)


if __name__ == '__main__':
    unittest.main()
