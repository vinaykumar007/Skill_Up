#!/usr/bin/env python
"""
Simple launcher for the ETL pipeline
This ensures correct path setup before importing modules
"""
from scripts.run_pipeline import main
import sys
from pathlib import Path
import os

# Setup path before any local imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root.resolve()))
os.chdir(str(project_root))

# Now safe to import

if __name__ == "__main__":
    main()
