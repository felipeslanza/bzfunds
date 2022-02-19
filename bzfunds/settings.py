"""
bzfunds.settings
~~~~~~~~~~~~~~~~

This module consolidates all of the project's settings and can be
customize accordingly. Some variables are exposed/sourced as
environment variables for convenience (but can be altered directly
as well).
"""

import os


__all__ = ("LOGGING_LEVEL", "LOGGING_FORMAT", "MONGODB")


# General
# ----
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "INFO")
LOGGING_FORMAT = "%(levelname)s - bzfunds.%(module)s.%(funcName)s - %(message)s"


# MongoDB
# ----
MONGODB = {
    "host": "localhost",
    "port": 27017,
    "db": "bzfundsDB",
    "collection": "funds",
    "username": os.environ.get("MONGODB_USERNAME"),
    "password": os.environ.get("MONGODB_PASSWORD"),
}
