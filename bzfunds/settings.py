"""
bzfunds.settings
~~~~~~~~~~~~~~~~

This module consolidates all of the project's settings and can be
customize accordingly. Some variables are exposed/sourced as
environment variables for convenience (but can be altered directly
as well).
"""

import os


__all__ = ("LOGGING_LEVEL", "MONGODB")


# General
# ----
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "debug")


# MongoDB
# ----
MONGODB = {
    # "host": "cluster0.18fme.mongodb.net",
    "host": "localhost",
    "port": 27017,
    "db": "bzfundsDB",
    "collection": "funds",
    "username": os.environ.get("MONGODB_USERNAME"),
    "password": os.environ.get("MONGODB_PASSWORD"),
}
