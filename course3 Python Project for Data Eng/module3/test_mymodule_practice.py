import unittest
from mymodule_practice import add  # Import the add function

class TestAddFunction(unittest.TestCase):
    """Unit tests for the `add` function."""

    def test_add_integers(self):
        """Test addition of integers."""
        self.assertEqual(add(2, 4), 6)  # 2 + 4 = 6
        self.assertEqual(add(0, 0), 0)  # 0 + 0 = 0
        self.assertNotEqual(add(-2, -2), 0)  # -2 + (-2) ≠ 0

    def test_add_floats(self):
        """Test addition of floating-point numbers."""
        self.assertEqual(add(2.3, 3.6), 5.9)  # 2.3 + 3.6 = 5.9
        self.assertEqual(add(2.3000, 4.300), 6.6)  # 2.3 + 4.3 = 6.6

    def test_add_strings(self):
        """Test concatenation of strings."""
        self.assertEqual(add("hello", "world"), "helloworld")  # "hello" + "world" = "helloworld"

if __name__ == "__main__":
    unittest.main()

