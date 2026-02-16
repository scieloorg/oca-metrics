import unittest

from oca_metrics.utils.normalization import (
    extract_year,
    stz_doi,
    stz_title,
)


class TestNormalization(unittest.TestCase):

    def test_stz_doi(self):
        self.assertEqual(stz_doi("10.1590/S0101-01012024000100001"), "10.1590/s0101-01012024000100001")
        self.assertEqual(stz_doi("https://doi.org/10.1590/123"), "10.1590/123")
        self.assertEqual(stz_doi("doi:10.1590/123"), "10.1590/123")
        self.assertEqual(stz_doi(None), "")
        self.assertEqual(stz_doi(""), "")

    def test_stz_title(self):
        self.assertEqual(stz_title("Título com Acentuação"), "titulocomacentuacao")
        self.assertEqual(stz_title("Multiple   Spaces"), "multiplespaces")
        self.assertEqual(stz_title(""), "")
        self.assertEqual(stz_title(None), "")

    def test_extract_year(self):
        self.assertEqual(extract_year("2024"), 2024)
        self.assertEqual(extract_year(2023), 2023)
        self.assertEqual(extract_year("invalid"), None)
        self.assertEqual(extract_year("1799"), 1799)
        self.assertEqual(extract_year("2027"), 2027)
        self.assertEqual(extract_year(None), None)


if __name__ == '__main__':
    unittest.main()
