#!/usr/bin/env python3
"""
Standalone script to refresh Caps Edge data.
Called by cron or manually for data updates.
"""

import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.fetcher import refresh_data


def main():
    print(f"[{datetime.now().isoformat()}] Starting Caps Edge data refresh...")

    try:
        players_updated = refresh_data()
        print(f"[{datetime.now().isoformat()}] Refresh complete. Updated {players_updated} players.")
        return 0
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] ERROR: Refresh failed - {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
