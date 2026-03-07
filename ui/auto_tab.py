"""Compatibility exports for automation-related UI modules."""

from ui.auto_processing import process_and_clean_data_final, sanitize_filename
from ui.auto_product_tab import AutomationFunction
from ui.auto_sublot_tab import SublotAutomationFunction

__all__ = [
    "AutomationFunction",
    "SublotAutomationFunction",
    "sanitize_filename",
    "process_and_clean_data_final",
]
