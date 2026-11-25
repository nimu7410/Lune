"""Test suite for the bot.

A package rather than a loose folder so `unittest discover` can be pointed at
`tests/` while still resolving the production modules, which live one level up
and are imported by their bare names (`import db_connector`, not
`import neobot.db_connector`).
"""

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests

# The leg builders read the exchanges' contract masters over HTTP. No test may
# depend on the network: one that needs a master patches its loader.
mock.patch.object(requests, "get", side_effect=OSError("tests are offline")).start()
