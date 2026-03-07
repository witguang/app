import os
import threading
import tkinter as tk
from tkinter import ttk, messagebox

from database import DatabaseManager
from ui.auto_tab import AutomationFunction, SublotAutomationFunction
from ui.report_tab import DataReportFunction
from ui.topo_tab import TopoDataFunction
from ui.trace_tab import SublotTraceFunction


class FPAnalysisApp:
    """Main application class."""

    def __init__(self, root):
        self.root = root
        self.root.title("抛光数据分析@吴广&唐家琦")
        self.root.geometry("1400x850")
        self.root.option_add("*Font", ("Microsoft YaHei UI", 10))
        self.style = ttk.Style()
        self.style.configure(".", font=("Microsoft YaHei UI", 10))
        self.style.configure("Treeview.Heading", font=("Microsoft YaHei UI", 10, "bold"))

        self.active_thread = None
        self.stop_event = threading.Event()

        main_pane = ttk.PanedWindow(root, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.left_frame = ttk.Frame(main_pane, width=200)
        main_pane.add(self.left_frame, weight=1)

        self.right_frame = ttk.Frame(main_pane)
        main_pane.add(self.right_frame, weight=5)

        self.topo_data = TopoDataFunction(self)
        self.sublot_trace = SublotTraceFunction(self)
        self.automation = AutomationFunction(self)
        self.sublot_automation = SublotAutomationFunction(self)
        self.data_report = DataReportFunction(self)
        self.current_function = None

        self.create_left_panel()
        self.show_function("\u57fa\u4e8e Sublot \u81ea\u52a8\u5316")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.warmup_db()

    def warmup_db(self):
        import time

        def update_status_label(message):
            for func in [self.sublot_automation, self.automation, self.sublot_trace]:
                if hasattr(func, "trace_progress_label") and func.trace_progress_label.winfo_exists():
                    func.trace_progress_label.config(text=message)
                    break

        update_status_label("\u6b63\u5728\u8fde\u63a5\u6570\u636e\u5e93\uff0c\u8bf7\u7a0d\u5019...")

        def _warmup():
            started_at = time.time()
            try:
                conn = DatabaseManager.get_db_connection(silent=True)
                if conn:
                    conn.close()
                elapsed = time.time() - started_at
                print(f"\n[DB \u63a2\u9488] Database warm-up successful (JVM started). \u8017\u65f6: {elapsed:.2f} \u79d2")
                try:
                    self.root.after(0, lambda: update_status_label(f"\u6570\u636e\u5e93\u5df2\u5c31\u7eea (\u9884\u70ed\u8017\u65f6: {elapsed:.2f}s)"))
                except Exception:
                    pass
            except Exception as exc:
                elapsed = time.time() - started_at
                print(f"\n[DB \u63a2\u9488] Database warm-up failed after {elapsed:.2f}s: {exc}")
                try:
                    self.root.after(0, lambda: update_status_label(f"\u6570\u636e\u5e93\u521d\u59cb\u5316\u5931\u8d25: {exc}"))
                except Exception:
                    pass

        threading.Thread(target=_warmup, daemon=True).start()

    def create_left_panel(self):
        header_font = ("Microsoft YaHei UI", 12, "bold")
        button_font = ("Microsoft YaHei UI", 10)
        ttk.Label(self.left_frame, text="\u529f\u80fd\u9009\u62e9", font=header_font).pack(pady=10, padx=10)
        functions = [
            "\u57fa\u4e8e Sublot \u81ea\u52a8\u5316",
            "\u57fa\u4e8e Product \u81ea\u52a8\u5316",
            "TOPO DATA",
            "Sublot \u5386\u53f2\u8ffd\u6eaf",
            "\u6570\u636e\u62a5\u8868\u5de5\u5177",
        ]
        style = ttk.Style()
        style.configure("LeftPanel.TButton", font=button_font)
        for func in functions:
            btn = ttk.Button(self.left_frame, text=func, style="LeftPanel.TButton", command=lambda selected=func: self.show_function(selected))
            btn.pack(pady=5, padx=10, fill=tk.X)

    def show_function(self, function_name):
        if self.active_thread and self.active_thread.is_alive():
            messagebox.showwarning("\u7e41\u5fd9", "\u4e00\u4e2a\u4efb\u52a1\u6b63\u5728\u8fd0\u884c\uff0c\u8bf7\u7b49\u5f85\u6216\u53d6\u6d88\u540e\u518d\u5207\u6362\u529f\u80fd\u3002")
            return

        for widget in self.right_frame.winfo_children():
            widget.destroy()

        self.current_function = function_name
        if function_name == "TOPO DATA":
            self.topo_data.show()
        elif function_name == "Sublot \u5386\u53f2\u8ffd\u6eaf":
            self.sublot_trace.show()
        elif function_name == "\u57fa\u4e8e Product \u81ea\u52a8\u5316":
            self.automation.show()
        elif function_name == "\u57fa\u4e8e Sublot \u81ea\u52a8\u5316":
            self.sublot_automation.show()
        elif function_name == "\u6570\u636e\u62a5\u8868\u5de5\u5177":
            self.data_report.show()

    def start_thread(self, target_func, control_func):
        if self.active_thread and self.active_thread.is_alive():
            messagebox.showwarning("\u7e41\u5fd9", "\u53e6\u4e00\u4e2a\u4efb\u52a1\u5df2\u5728\u8fd0\u884c\u3002")
            return

        self.stop_event.clear()
        control_func(False)

        def thread_wrapper():
            try:
                target_func()
            except Exception as exc:
                import traceback

                traceback.print_exc()
                self.root.after(0, messagebox.showerror, "\u7ebf\u7a0b\u9519\u8bef", f"\u540e\u53f0\u7ebf\u7a0b\u53d1\u751f\u610f\u5916\u9519\u8bef: {exc}")
            finally:
                if self.root.winfo_exists():
                    self.root.after(0, control_func, True)
                self.active_thread = None

        self.active_thread = threading.Thread(target=thread_wrapper, daemon=True)
        self.active_thread.start()

    def update_progress(self, message: str, value: float = None, feature_id: str = ""):
        if not self.root.winfo_exists():
            return

        if self.current_function == "TOPO DATA" and feature_id == "topo":
            self.topo_data.topo_progress_label.config(text=message)
            if value is not None:
                self.topo_data.topo_progress["value"] = value
        elif self.current_function == "Sublot \u5386\u53f2\u8ffd\u6eaf" and feature_id == "trace":
            self.sublot_trace.progress_label.config(text=message)
        elif self.current_function == "\u57fa\u4e8e Product \u81ea\u52a8\u5316":
            if feature_id == "auto_trace":
                self.automation.trace_progress_label.config(text=message)
            elif feature_id == "auto_topo" and hasattr(self.automation, "topo_progress_label"):
                self.automation.topo_progress_label.config(text=message)
        elif self.current_function == "\u57fa\u4e8e Sublot \u81ea\u52a8\u5316":
            if feature_id == "auto_trace":
                self.sublot_automation.trace_progress_label.config(text=message)
            elif feature_id == "auto_topo" and hasattr(self.sublot_automation, "topo_progress_label"):
                self.sublot_automation.topo_progress_label.config(text=message)

        self.root.update_idletasks()

    def on_close(self):
        if self.active_thread and self.active_thread.is_alive():
            if not messagebox.askyesno("\u9000\u51fa\u786e\u8ba4", "\u5f53\u524d\u4ecd\u6709\u4efb\u52a1\u5728\u540e\u53f0\u8fd0\u884c\uff0c\u786e\u5b9a\u8981\u9000\u51fa\u5417\uff1f"):
                return

        self.stop_event.set()
        try:
            self.root.destroy()
        except tk.TclError:
            pass
        finally:
            os._exit(0)
