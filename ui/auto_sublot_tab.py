import tkinter as tk
from tkinter import ttk, messagebox

from ui.auto_product_tab import AutomationFunction

class SublotAutomationFunction:
    """Sublot ID-based automated processing workflow."""
    def __init__(self, app):
        self.app = app
        self.frame = None
        self.trace_results = []
        self.search_prefix = ""
        self.log_messages = []
        self.output_base_dir = ""

    def show(self):
        """Shows the Sublot automation UI."""
        if self.frame: self.frame.destroy()
        self.frame = ttk.Frame(self.app.right_frame)
        self.frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        ttk.Label(self.frame, text="自动化处理流程 (基于Sublot)", font=('Arial', 14, 'bold')).pack(pady=5)
        
        main_paned_window = ttk.PanedWindow(self.frame, orient=tk.VERTICAL)
        main_paned_window.pack(fill=tk.BOTH, expand=True)

        trace_frame_container = ttk.Frame(main_paned_window, height=350)
        main_paned_window.add(trace_frame_container, weight=3)
        self._create_trace_ui(trace_frame_container)

        topo_frame_container = ttk.Frame(main_paned_window, height=300)
        main_paned_window.add(topo_frame_container, weight=2)
        
        AutomationFunction._create_topo_ui(self, topo_frame_container)

    def _create_trace_ui(self, parent):
        """Creates the trace UI for the Sublot automation."""
        trace_frame = ttk.LabelFrame(parent, text="步骤 1: 追溯Sublot")
        trace_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        left_frame = ttk.Frame(trace_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, pady=5)
        right_frame = ttk.Frame(trace_frame)
        right_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.sublot_id_frame = ttk.LabelFrame(left_frame, text="Sublot ID(s) (逗号分隔)")
        self.sublot_id_frame.pack(fill=tk.X, pady=5)
        self.sublot_id_entry = ttk.Entry(self.sublot_id_frame, width=25)
        self.sublot_id_entry.pack(padx=5, pady=5)

        action_frame = ttk.Frame(left_frame)
        action_frame.pack(fill=tk.X, pady=10)
        self.trace_run_button = ttk.Button(action_frame, text="执行追溯", command=self._start_trace_thread)
        self.trace_run_button.pack(fill=tk.X)
        
        # --- 新增: 导出追溯结果 CSV 按钮 ---
        self.export_button = ttk.Button(action_frame, text="导出追溯结果 CSV", command=self.export_trace_results_to_csv, state=tk.DISABLED)
        self.export_button.pack(fill=tk.X, pady=(5,0))

        self.transfer_button = ttk.Button(action_frame, text="⬇️ 将结果填充至TOPO ⬇️", command=self._transfer_data_to_topo, state=tk.DISABLED)
        self.transfer_button.pack(fill=tk.X, pady=(5,0))

        self.save_thk_button = ttk.Button(action_frame, text="💾 保存THK并执行计算 📈", command=self._start_thk_save_and_calc_thread, state=tk.DISABLED)
        self.save_thk_button.pack(fill=tk.X, pady=(5,0))


        results_frame = ttk.LabelFrame(right_frame, text="追溯结果")
        results_frame.pack(fill=tk.BOTH, expand=True)
        
        cols = ('PROD_ID', 'SUBLOT_ID', 'DPOL_EQP', 'DPOL_TIME', 'DPGE_EQP', 'DPGE_TIME', 'FPOL_EQP', 'FPOL_TIME', 'FPMS_EQP', 'FPMS_TIME')
        self.trace_tree = ttk.Treeview(results_frame, columns=cols, show='headings', height=8)
        for col in cols: 
            self.trace_tree.heading(col, text=col)
            width = 180 if "TIME" in col else 100
            self.trace_tree.column(col, width=width, anchor='center')
        
        ysb = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.trace_tree.yview)
        xsb = ttk.Scrollbar(results_frame, orient=tk.HORIZONTAL, command=self.trace_tree.xview)
        self.trace_tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        
        self.trace_tree.grid(row=0, column=0, sticky='nsew')
        ysb.grid(row=0, column=1, sticky='ns')
        xsb.grid(row=1, column=0, sticky='ew')
        results_frame.grid_rowconfigure(0, weight=1)
        results_frame.grid_columnconfigure(0, weight=1)
        
        self.trace_progress_label = ttk.Label(right_frame, text="请输入Sublot ID后执行追溯...")
        self.trace_progress_label.pack(side=tk.BOTTOM, fill=tk.X, pady=(5,0))

    def _start_trace_thread(self):
        """Starts the trace process in a background thread for the Sublot workflow."""
        def trace_logic():
            sublot_ids_str = self.sublot_id_entry.get().strip()
            if not sublot_ids_str:
                messagebox.showwarning("输入错误", "请输入至少一个Sublot ID或前缀。")
                return
            
            self.search_prefix = sublot_ids_str

            params = {
                "sublot_ids": [s.strip() for s in sublot_ids_str.split(',') if s.strip()],
                "selected_prod_id": None, 
                "selected_eqp": None,
                "time_mode": None
            }

            self.app.update_progress("正在追溯...", None, 'auto_trace')
            self.trace_results.clear()

            results = self.app.sublot_trace.run_database_query(**params)
            self.trace_results = results or []
            self.app.root.after(0, self._display_trace_results)

        self.app.start_thread(trace_logic, self._set_trace_controls_state)
    _run_topo_logic = AutomationFunction._run_topo_logic
    _set_trace_controls_state = AutomationFunction._set_trace_controls_state
    _start_topo_thread = AutomationFunction._start_topo_thread
    _set_topo_controls_state = AutomationFunction._set_topo_controls_state
    _transfer_data_to_topo = AutomationFunction._transfer_data_to_topo
    _display_trace_results = AutomationFunction._display_trace_results
    _select_rd_tools_auto = AutomationFunction._select_rd_tools_auto
    _select_non_rd_tools_auto = AutomationFunction._select_non_rd_tools_auto
    _perform_esfqr_replacement = AutomationFunction._perform_esfqr_replacement

    _start_thk_save_and_calc_thread = AutomationFunction._start_thk_save_and_calc_thread
    _run_thk_sector_copy_logic = AutomationFunction._run_thk_sector_copy_logic
    _run_thk_calculation_logic = AutomationFunction._run_thk_calculation_logic

    _get_mapped_dpge_id = AutomationFunction._get_mapped_dpge_id
    _find_primary_sector_file = AutomationFunction._find_primary_sector_file
    _search_fallback_paths = AutomationFunction._search_fallback_paths
    
    # --- 新增: 将导出方法绑定到 SublotAutomationFunction ---
    export_trace_results_to_csv = AutomationFunction.export_trace_results_to_csv

