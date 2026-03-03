import os
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from database import DatabaseManager

from ui.topo_tab import TopoDataFunction
from ui.trace_tab import SublotTraceFunction
from ui.auto_tab import AutomationFunction, SublotAutomationFunction
from ui.report_tab import DataReportFunction

class FPAnalysisApp:
    """Main application class."""
    def __init__(self, root):
        self.root = root
        self.root.title("抛光数据分析@吴广&唐家琦)")
        self.root.geometry("1400x850")
        
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
        self.show_function('Sublot自动化流程')
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def create_left_panel(self):
        ttk.Label(self.left_frame, text="功能选择", font=('Arial', 12, 'bold')).pack(pady=10, padx=10)
        functions = ['Sublot自动化流程', 'Product自动化处理', 'TOPO DATA', 'Trace Sublot History', 'Data Report Tool']
        for func in functions:
            btn = ttk.Button(self.left_frame, text=func, command=lambda f=func: self.show_function(f))
            btn.pack(pady=5, padx=10, fill=tk.X)

    def show_function(self, function_name):
        if self.active_thread and self.active_thread.is_alive():
            messagebox.showwarning("繁忙", "一个任务正在运行。请等待或取消它再切换功能。")
            return
            
        for widget in self.right_frame.winfo_children():
            widget.destroy()
        
        self.current_function = function_name
        if function_name == 'TOPO DATA':
            self.topo_data.show()
        elif function_name == 'Trace Sublot History':
            self.sublot_trace.show()
        elif function_name == 'Product自动化处理':
            self.automation.show()
        elif function_name == 'Sublot自动化流程':
            self.sublot_automation.show()
        elif function_name == 'Data Report Tool':
            self.data_report.show()

    def start_thread(self, target_func, control_func):
        if self.active_thread and self.active_thread.is_alive():
            messagebox.showwarning("繁忙", "另一个进程已在运行。")
            return
        
        self.stop_event.clear()
        control_func(False)

        def thread_wrapper():
            try:
                target_func()
            except Exception as e:
                import traceback
                traceback.print_exc()
                self.root.after(0, messagebox.showerror, "线程错误", f"后台线程发生意外错误: {e}")
            finally:
                if self.root.winfo_exists():
                    self.root.after(0, control_func, True)
                self.active_thread = None

        self.active_thread = threading.Thread(target=thread_wrapper, daemon=True)
        self.active_thread.start()

    def update_progress(self, message: str, value: float = None, feature_id: str = ''):
        if not self.root.winfo_exists(): return
        
        if self.current_function == 'TOPO DATA' and feature_id == 'topo':
             self.topo_data.topo_progress_label.config(text=message)
             if value is not None: self.topo_data.topo_progress['value'] = value
        elif self.current_function == 'Trace Sublot History' and feature_id == 'trace':
            self.sublot_trace.progress_label.config(text=message)
        elif self.current_function == 'Product自动化处理':
            if feature_id == 'auto_trace':
                self.automation.trace_progress_label.config(text=message)
            elif feature_id == 'auto_topo' and hasattr(self.automation, 'topo_progress_label'):
                self.automation.topo_progress_label.config(text=message)
        elif self.current_function == 'Sublot自动化流程':
            if feature_id == 'auto_trace':
                self.sublot_automation.trace_progress_label.config(text=message)
            elif feature_id == 'auto_topo' and hasattr(self.sublot_automation, 'topo_progress_label'):
                self.sublot_automation.topo_progress_label.config(text=message)
            
        self.root.update_idletasks()

    def on_close(self):
        if self.active_thread and self.active_thread.is_alive():
            if not messagebox.askyesno("退出确认", "一个任务仍在后台运行。您确定要强制退出吗？"):
                return

        self.stop_event.set()
        try:
            self.root.destroy()
        except tk.TclError:
            pass
        finally:
            os._exit(0)