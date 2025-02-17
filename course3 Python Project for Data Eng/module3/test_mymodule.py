import unittest
from mymodule import square, double  # Import functions correctly

class TestSquare(unittest.TestCase):
    """Unit tests for the `square` function."""

    def test_square(self):
        """Tests for the `square` function."""
        self.assertEqual(square(2), 4)  # 2² = 4
        self.assertEqual(square(3.0), 9.0)  # 3.0² = 9.0
        self.assertEqual(square(-3), 9)  # (-3)² = 9
        self.assertNotEqual(square(-3), -9)  # Ensure it doesn't return a negative result

class TestDouble(unittest.TestCase):
    """Unit tests for the `double` function."""

    def test_double(self):
        """Tests for the `double` function."""
        self.assertEqual(double(2), 4)  # 2 * 2 = 4
        self.assertEqual(double(-3.1), -6.2)  # -3.1 * 2 = -6.2
        self.assertEqual(double(0), 0)  # 0 * 2 = 0

if __name__ == "__main__":
    unittest.main()