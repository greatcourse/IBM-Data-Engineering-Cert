"""
This module provides a function to add two numbers and display the result.
"""

def add(number1: float, number2: float) -> float:
    """
    Returns the sum of number1 and number2.

    Args:
        number1 (float): The first number.
        number2 (float): The second number.

    Returns:
        float: The sum of the two numbers.
    """
    return number1 + number2

# Define constants
NUM1: int = 4
NUM2: int = 5  # Renamed to follow UPPER_CASE naming convention

# Calculate sum
total: int = add(NUM1, NUM2)

# Display result using an f-string for better readability
print(f"The sum of {NUM1} and {NUM2} is {total}")
