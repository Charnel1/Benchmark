from enum import Enum


class ComparisonState(Enum):
    """
    Represents a three-state boolean comparison
    """
    Wichtig = True      # True
    Unwichtig = False      # False
    Optional = None  # Don't Care


class ComparisonRating(Enum):
    Mittel = None,
    Schlecht = False
    Gut = True
