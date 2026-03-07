"""Compatibility entrypoint for the split application structure.

Historically this file contained the full monolithic implementation.
The code is now split across dedicated modules:
- `database.py`
- `ui/topo_tab.py`
- `ui/trace_tab.py`
- `ui/auto_tab.py`
- `ui/report_tab.py`
- `ui/main_window.py`

`python app.py` remains supported for backward compatibility.
"""

from database import DatabaseManager
from ui.auto_tab import (
    AutomationFunction,
    SublotAutomationFunction,
    process_and_clean_data_final,
    sanitize_filename,
)
from ui.main_window import FPAnalysisApp
from ui.report_tab import DataReportFunction
from ui.topo_tab import TopoDataFunction
from ui.trace_tab import SublotTraceFunction

__all__ = [
    "AutomationFunction",
    "DataReportFunction",
    "DatabaseManager",
    "FPAnalysisApp",
    "SublotAutomationFunction",
    "SublotTraceFunction",
    "TopoDataFunction",
    "process_and_clean_data_final",
    "sanitize_filename",
    "main",
]


def main():
    from main import main as run_main

    run_main()


if __name__ == "__main__":
    main()
