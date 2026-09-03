"""Fixed, internal values — not environment-configurable.

Unlike app/config.py (deployment knobs that legitimately vary per environment),
these are protocol/naming constants whose value must stay identical everywhere
they're used. Redis cache-key prefixes in particular must match exactly between
where an entry is written (app/api/search.py, app/api/files.py) and where it's
invalidated (app/worker.py) — centralizing them here prevents that drifting into
two out-of-sync string literals.
"""

SEARCH_CACHE_PREFIX = "search"
SECTION_CACHE_PREFIX = "section"
