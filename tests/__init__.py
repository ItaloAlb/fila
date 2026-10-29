"""Test package. Run from the repository root with:

    python -m unittest discover -s tests -t .
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
