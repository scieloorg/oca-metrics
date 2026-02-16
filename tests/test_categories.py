import unittest
import os

from oca_metrics.utils.categories import load_categories


class TestCategories(unittest.TestCase):

    def setUp(self):
        # Create dummy category files for testing
        self.test_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "oca_metrics", "data", "categories")
        os.makedirs(self.test_dir, exist_ok=True)
        
        with open(os.path.join(self.test_dir, "test_level.txt"), "w") as f:
            f.write('"Cat1"\n"Cat2"\n')

    def tearDown(self):
        # Clean up
        test_file = os.path.join(self.test_dir, "test_level.txt")
        if os.path.exists(test_file):
            os.remove(test_file)

    def test_load_categories_success(self):
        cats = load_categories("test_level")
        self.assertEqual(len(cats), 2)
        self.assertIn("Cat1", cats)
        self.assertIn("Cat2", cats)

    def test_load_categories_not_found(self):
        cats = load_categories("non_existent_level")
        self.assertEqual(len(cats), 0)


if __name__ == '__main__':
    unittest.main()
