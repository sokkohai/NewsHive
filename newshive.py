#!/usr/bin/env python
"""newshive CLI wrapper for direct execution."""

import sys

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # If python-dotenv is not installed, continue without it
    pass

from src.cli import main

if __name__ == "__main__":
    sys.exit(main())
