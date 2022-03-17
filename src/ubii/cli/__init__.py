"""
Provides the default node implementation as a CLI script in the :mod:`.main` module, and a
helper to discover installed processing modules.
"""
from .main import load_pm_entry_points

__all__ = (
    'load_pm_entry_points',
)
