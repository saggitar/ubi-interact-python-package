"""
Blub
"""

try:
    from importlib import metadata
except ImportError:  # for Python<3.8
    import importlib_metadata as metadata


def load_pm_entry_points():
    return [entry.load() for entry in metadata.entry_points()['ubii.processing_modules']]
