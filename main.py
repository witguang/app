import tkinter as tk

from database import DatabaseManager
from ui.main_window import FPAnalysisApp


def main():
    root = tk.Tk()

    try:
        root.iconbitmap(DatabaseManager.get_resource_path("icon.ico"))
    except Exception:
        pass

    FPAnalysisApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
