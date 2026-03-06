"""Entry point for running newshive as a module.

Supports one invocation method:
  python -m src pipeline    # Pipeline only
"""

import sys

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # If python-dotenv is not installed, continue without it
    pass

from .cli import main

if __name__ == "__main__":
    main()
