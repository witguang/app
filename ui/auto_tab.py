import os
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, Checkbutton
from tkcalendar import DateEntry
import glob
import shutil
from datetime import datetime
import re
import csv
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from config import Config
from utils import FileProcessor

class AutomationFunction:
    def __init__(self, app):
        self.app = app
        self.frame = None
        self.trace_results = []
        self.log_messages = []
        self.output_base_dir = ""
        self.all_prods = []

    def show(self):
        if self.frame: self.frame.destroy()
        self.frame = ttk.Frame(self.app.right_frame)
        self.frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        ttk.Label(self.frame, text="Product自动化处理流程 (基于时间)", font=('Arial', 14, 'bold')).pack(pady=5)
        
        main_paned_window = ttk.PanedWindow(self.frame, orient=tk.VERTICAL)
        main_paned_window.pack(fill=tk.BOTH, expand=True)

        trace_frame_container = ttk.Frame(main_paned_window, height=350)
        main_paned_window.add(trace_frame_container, weight=3)
        self._create_trace_ui(trace_frame_container)

        topo_frame_container = ttk.Frame(main_paned_window, height=300)
        main_paned_window.add(topo_frame_container, weight=2)
        self._create_topo_ui(topo_frame_container)

    def _create_trace_ui(self, parent):
        trace_frame = ttk.LabelFrame(parent, text="步骤 1: 追溯Sublot和DP Data")
        trace_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        left_frame = ttk.Frame(trace_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, pady=5)
        right_frame = ttk.Frame(trace_frame)
        right_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        time_frame = ttk.LabelFrame(left_frame, text="时间范围")
        time_frame.pack(fill=tk.X, pady=5)
        self.trace_time_mode = tk.StringVar(value="recent")
        ttk.Radiobutton(time_frame, text="最近24小时", variable=self.trace_time_mode, value="recent", command=self._toggle_trace_time_controls).pack(anchor=tk.W)
        ttk.Radiobutton(time_frame, text="自定义", variable=self.trace_time_mode, value="custom", command=self._toggle_trace_time_controls).pack(anchor=tk.W)
        self.trace_start_date = DateEntry(time_frame, date_pattern='yyyy-MM-dd'); self.trace_start_date.pack(pady=2)
        self.trace_end_date = DateEntry(time_frame, date_pattern='yyyy-MM-dd'); self.trace_end_date.pack(pady=2)

        prod_frame = ttk.LabelFrame(left_frame, text="产品筛选 (可手动输入/搜索)")
        prod_frame.pack(fill=tk.X, pady=5)
        
        self.all_prods = ['ALL', 
                         'EPCRAST035-A2','EPCRAST036-A2','EPCRAST037-A2','EPCRAST045-A2',
                         'EPCRAST042-A2','EPCRAST043-A2','EPCRAST044-A2','EPCRAST046-A2',
                         'EPCRAST047-A2','EPCRAST048-A2',
                         'SPCRAST002-A2',
                         'EPCRAST012-A2','EPCRAST013-A2',
                         'PPCS90A006-A2', 'PPCS90A007-A2', 'EPCSH1A006-A2', 'EPCSH1A007-A2','EPCSJ0C005-A2','EPCSG0A007-A2','PPCPM1A014-A2','PPCTAST009-A2']
        
        self.trace_prod_id_combo = ttk.Combobox(prod_frame, values=self.all_prods)
        self.trace_prod_id_combo.pack(padx=5, pady=5, fill=tk.X)
        self.trace_prod_id_combo.set('ALL')
        self.trace_prod_id_combo.bind('<KeyRelease>', self._filter_prod_combo)

        limit_frame = ttk.LabelFrame(left_frame, text="Sublot 数量限定")
        limit_frame.pack(fill=tk.X, pady=5)
        self.sublot_limit_var = tk.StringVar(value="5")
        self.sublot_limit_entry = ttk.Entry(limit_frame, textvariable=self.sublot_limit_var, width=10)
        self.sublot_limit_entry.pack(padx=5, pady=5)

        eqp_frame = ttk.LabelFrame(left_frame, text="目标设备 (FPOL)")
        eqp_frame.pack(fill=tk.X, pady=5)

        listbox_frame = ttk.Frame(eqp_frame)
        listbox_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, pady=5)

        self.trace_eqp_listbox = tk.Listbox(listbox_frame, selectmode=tk.MULTIPLE, height=5, exportselection=False)
        self.trace_eqp_listbox.pack(side=tk.LEFT, fill=tk.Y)
        
        scrollbar = ttk.Scrollbar(listbox_frame, orient=tk.VERTICAL, command=self.trace_eqp_listbox.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.trace_eqp_listbox.config(yscrollcommand=scrollbar.set)
        
        rd_eqp = ['FPOL007', 'FPOL008', 'FPOL009', 'FPOL010']
        other_eqp = [f'FPOL{i:03d}' for i in range(1, 17) if f'FPOL{i:03d}' not in rd_eqp]
        eqp_options = rd_eqp + other_eqp
        for eqp in eqp_options: self.trace_eqp_listbox.insert(tk.END, eqp)

        eqp_button_frame = ttk.Frame(eqp_frame)
        eqp_button_frame.pack(side=tk.LEFT, padx=5, fill=tk.Y)
        ttk.Button(eqp_button_frame, text="所有机台", command=lambda: self.trace_eqp_listbox.selection_set(0, tk.END)).pack(pady=2, fill=tk.X)
        ttk.Button(eqp_button_frame, text="研发机台", command=self._select_rd_tools_auto).pack(pady=2, fill=tk.X)
        ttk.Button(eqp_button_frame, text="非研发机台", command=self._select_non_rd_tools_auto).pack(pady=2, fill=tk.X)
        ttk.Button(eqp_button_frame, text="取消选择", command=lambda: self.trace_eqp_listbox.selection_clear(0, tk.END)).pack(pady=2, fill=tk.X)

        action_frame = ttk.Frame(left_frame)
        action_frame.pack(fill=tk.X, pady=10)
        self.trace_run_button = ttk.Button(action_frame, text="执行追溯", command=self._start_trace_thread)
        self.trace_run_button.pack(fill=tk.X)
        
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
        
        self.trace_progress_label = ttk.Label(right_frame, text="请设置参数后执行追溯...")
        self.trace_progress_label.pack(side=tk.BOTTOM, fill=tk.X, pady=(5,0))

        self._toggle_trace_time_controls()

    def _filter_prod_combo(self, event):
        if event.keysym in ['Up', 'Down', 'Left', 'Right', 'Return', 'Escape']:
            return

        current_text = self.trace_prod_id_combo.get().strip()
        
        if not current_text:
            self.trace_prod_id_combo['values'] = self.all_prods
            return

        filtered_values = [item for item in self.all_prods if current_text.lower() in item.lower()]
        self.trace_prod_id_combo['values'] = filtered_values

    def _select_rd_tools_auto(self):
        self.trace_eqp_listbox.selection_clear(0, tk.END)
        rd_tools = {'FPOL007', 'FPOL008', 'FPOL009', 'FPOL010'}
        all_items = self.trace_eqp_listbox.get(0, tk.END)
        for i, item in enumerate(all_items):
            if item in rd_tools:
                self.trace_eqp_listbox.selection_set(i)

    def _select_non_rd_tools_auto(self):
        self.trace_eqp_listbox.selection_clear(0, tk.END)
        rd_tools = {'FPOL007', 'FPOL008', 'FPOL009', 'FPOL010'}
        all_items = self.trace_eqp_listbox.get(0, tk.END)
        for i, item in enumerate(all_items):
            if item not in rd_tools:
                self.trace_eqp_listbox.selection_set(i)

    def _create_topo_ui(self, parent):
        topo_frame = ttk.LabelFrame(parent, text="步骤 2: TOPO 分析 (参数将由追溯结果自动填充)")
        topo_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        params_frame = ttk.Frame(topo_frame)
        params_frame.pack(fill=tk.X, pady=5)

        date_frame = ttk.Frame(params_frame)
        date_frame.pack(side=tk.LEFT, padx=10)
        ttk.Label(date_frame, text="开始日期:").pack(anchor=tk.W)
        self.topo_start_date = DateEntry(date_frame, date_pattern='yyyy-mm-dd'); self.topo_start_date.pack()
        ttk.Label(date_frame, text="结束日期:").pack(anchor=tk.W, pady=(5,0))
        self.topo_end_date = DateEntry(date_frame, date_pattern='yyyy-mm-dd'); self.topo_end_date.pack()

        device_frame = ttk.Frame(params_frame)
        device_frame.pack(side=tk.LEFT, padx=10)
        ttk.Label(device_frame, text="FPMS 机台:").pack(anchor=tk.W)
        self.topo_device_listbox = tk.Listbox(device_frame, selectmode=tk.MULTIPLE, exportselection=0, height=5)
        self.topo_device_listbox.pack()
        
        device_list = ["DPGE101"] + [f"FPMS{num:03d}" for num in range(1, 13)]
        for device in device_list: 
            self.topo_device_listbox.insert(tk.END, device)
            
        self.topo_device_listbox.selection_set(0, tk.END)

        entry_frame = ttk.Frame(params_frame)
        entry_frame.pack(side=tk.LEFT, padx=10, fill=tk.Y)
        ttk.Label(entry_frame, text="Lot ID (逗号分隔):").pack(anchor=tk.W)
        self.topo_lot_id_entry = ttk.Entry(entry_frame, width=40); self.topo_lot_id_entry.pack()
        ttk.Label(entry_frame, text="文件前缀:").pack(anchor=tk.W, pady=(5,0))
        self.topo_file_prefix_combo = ttk.Combobox(entry_frame, values=Config.IMP_PREFIXES, state="readonly", width=38)
        self.topo_file_prefix_combo.pack(); self.topo_file_prefix_combo.current(1)
        
        options_frame = ttk.Frame(entry_frame)
        options_frame.pack(anchor=tk.W, pady=5)
        self.topo_export_profile_var = tk.BooleanVar(value=True)
        Checkbutton(options_frame, text="导出Thickness Profile", variable=self.topo_export_profile_var).pack(side=tk.LEFT, padx=5)
        self.topo_fpms007_dp_var = tk.BooleanVar(value=True)
        Checkbutton(options_frame, text="FPMS007为DP测试", variable=self.topo_fpms007_dp_var).pack(side=tk.LEFT, padx=5)
        
        self.topo_process_both_var = tk.BooleanVar(value=False)
        Checkbutton(options_frame, text="自动处理 EE1 & EE2 (先EE2)", variable=self.topo_process_both_var).pack(side=tk.LEFT, padx=5)

        action_frame = ttk.Frame(topo_frame)
        action_frame.pack(fill=tk.X, side=tk.BOTTOM, pady=5)
        self.topo_run_button = ttk.Button(action_frame, text="开始TOPO分析", command=self._start_topo_thread, state=tk.DISABLED)
        self.topo_run_button.pack(side=tk.LEFT, padx=10)
        self.topo_progress_label = ttk.Label(action_frame, text="等待从上方填充参数...")
        self.topo_progress_label.pack(side=tk.LEFT, padx=10)

    def _get_mapped_dpge_id(self, dpge_eqp_id: str) -> str:
        if dpge_eqp_id == 'DPGE003':
            return 'FPMS004'
        elif dpge_eqp_id == 'DPGE004':
            return 'FPMS007'
        elif dpge_eqp_id == 'DPGE101':
            return 'DPGE101'
        else:
            return dpge_eqp_id 

    def _find_primary_sector_file(self, eqp_type: str, eqp_id: Optional[str], eqp_time: Any, sublot_id: str) -> Optional[Dict[str, str]]:
        if not eqp_id or not eqp_time:
            return None

        search_eqp_id = eqp_id
        try:
            if isinstance(eqp_time, str):
                time_obj = datetime.strptime(eqp_time.split('.')[0], '%Y-%m-%d %H:%M:%S')
            elif isinstance(eqp_time, datetime):
                time_obj = eqp_time
            else:
                return None 
            
            current_date = time_obj.date()
            date_str = current_date.strftime('%Y%m%d')

            if eqp_type == 'DPGE':
                search_eqp_id = self._get_mapped_dpge_id(eqp_id)

            if search_eqp_id == 'DPGE101':
                search_base = os.path.join(Config.DPGE101_BASE_PATH, date_str)
            elif current_date >= Config.PATH_TRANSITION_DATE:
                base = Config.NEW_BASE_PATH[0]
                search_base = os.path.join(base, f"01_{search_eqp_id}", "01_Production", date_str)
            else:
                search_base = os.path.join(Config.OLD_BASE_PATH, f"02_{search_eqp_id}", "01_Production", date_str)
            
            if not os.path.isdir(search_base):
                return None

            sublot_folders = glob.glob(os.path.join(search_base, f"{sublot_id}*"))
            if not sublot_folders:
                return None
            
            sublot_folder_path = sublot_folders[0]
            if not os.path.isdir(sublot_folder_path):
                 return None

            file_search_pattern = os.path.join(sublot_folder_path, f"{Config.THK_SECTOR_PREFIX}*.csv")
            found_files = glob.glob(file_search_pattern)
            
            if not found_files:
                return None

            source_file_path = found_files[0]
            source_file_name = os.path.basename(source_file_path)

            return {
                'source_path': source_file_path,
                'original_filename': source_file_name
            }

        except Exception as e:
            print(f"Error in _find_primary_sector_file for {sublot_id} ({eqp_type} -> {search_eqp_id}): {e}")
            self.log_messages.append(f"[查找错误] _find_primary_sector_file 失败: {e}")
            return None
    
    def _toggle_trace_time_controls(self):
        state = tk.NORMAL if self.trace_time_mode.get() == "custom" else tk.DISABLED
        self.trace_start_date.config(state=state)
        self.trace_end_date.config(state=state)

    def _start_trace_thread(self):
        self.app.start_thread(self._run_trace_logic, self._set_trace_controls_state)

    def _run_trace_logic(self):
        params = {
            "selected_eqp": [self.trace_eqp_listbox.get(i) for i in self.trace_eqp_listbox.curselection()],
            "selected_prod_id": self.trace_prod_id_combo.get(),
            "time_mode": self.trace_time_mode.get(),
            "start_date": self.trace_start_date.get_date(),
            "end_date": self.trace_end_date.get_date(),
            "sublot_ids": None
        }
        if not params["selected_eqp"]:
            messagebox.showwarning("输入错误", "请至少选择一个目标设备。")
            return

        self.app.update_progress("正在追溯...", None, 'auto_trace')
        self.trace_results.clear()

        results = self.app.sublot_trace.run_database_query(**params)
        
        try:
            limit_str = self.sublot_limit_var.get()
            if limit_str.strip().lower() in ["", "0", "all", "none"]:
                limit = None
            else:
                limit = int(limit_str)
        except ValueError:
            messagebox.showwarning("输入无效", f"无效的Sublot数量 '{self.sublot_limit_var.get()}'。将使用默认值 5。")
            limit = 5
            self.sublot_limit_var.set("5")

        if results and limit is not None and limit > 0:
            original_count = len(results)
            self.trace_results = results[:limit]
            self.app.root.after(0, messagebox.showinfo, "数量限定", f"已将追溯结果从 {original_count} 限定为 {len(self.trace_results)} 个 Sublots。")
        else:
            self.trace_results = results or []

        self.app.root.after(0, self._display_trace_results)

    def _display_trace_results(self):
        self.trace_tree.delete(*self.trace_tree.get_children())
        self.transfer_button.config(state=tk.DISABLED)
        self.save_thk_button.config(state=tk.DISABLED)
        self.export_button.config(state=tk.DISABLED) 

        if self.trace_results:
            try:
                num_cols_expected = len(self.trace_tree['columns'])
                valid_results_count = 0
                for row in self.trace_results:
                    if len(row) != num_cols_expected:
                        print(f"AutoTab - Column count mismatch! Expected {num_cols_expected}, got {len(row)}. Data: {row}")
                        continue
                    self.trace_tree.insert('', tk.END, values=[str(item).strip() if item is not None else "" for item in row])
                    valid_results_count += 1
                
                self.app.update_progress(f"追溯成功，找到 {valid_results_count} 条记录。", None, 'auto_trace')
                if valid_results_count > 0:
                    self.transfer_button.config(state=tk.NORMAL)
                    self.save_thk_button.config(state=tk.NORMAL)
                    self.export_button.config(state=tk.NORMAL) 

            except Exception as e:
                messagebox.showerror("显示错误", f"在自动化流程中显示追溯结果时出错: {e}")
        else:
            self.app.update_progress("追溯完成，未找到任何记录。", None, 'auto_trace')
            messagebox.showwarning("无结果", "根据当前条件，未能追溯到任何Sublot。")
    
    def export_trace_results_to_csv(self):
        filename = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV 文件", "*.csv")])
        if not filename: return
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                headers = [self.trace_tree.heading(col)['text'] for col in self.trace_tree['columns']]
                writer.writerow(headers)
                for iid in self.trace_tree.get_children():
                    writer.writerow(self.trace_tree.item(iid)['values'])
            messagebox.showinfo("成功", f"追溯结果已导出到:\n{filename}")
        except Exception as e:
            messagebox.showerror("导出失败", f"无法导出文件: {e}")

    def _transfer_data_to_topo(self):
        if not self.trace_results: messagebox.showerror("错误", "没有可供填充的追溯结果。"); return
        try:
            sublot_ids = sorted(list(set(row[1].strip() for row in self.trace_results)))
            self.topo_lot_id_entry.delete(0, tk.END); self.topo_lot_id_entry.insert(0, ",".join(sublot_ids))
            all_dpge_dates = []; all_fpms_dates = []; dpge_devices_found = set()
            for row in self.trace_results:
                if row[5]: 
                    try: dt_object = datetime.strptime(str(row[5]).split('.')[0], '%Y-%m-%d %H:%M:%S'); all_dpge_dates.append(dt_object)
                    except (ValueError, IndexError): continue
                if row[9]:
                    try: dt_object = datetime.strptime(str(row[9]).split('.')[0], '%Y-%m-%d %H:%M:%S'); all_fpms_dates.append(dt_object)
                    except (ValueError, IndexError): continue
                if row[4]: dpge_devices_found.add(row[4].strip())
            
            if not all_dpge_dates: raise ValueError("追溯结果中无有效的DPGE时间，无法确定TOPO分析开始日期。")
            
            if not all_fpms_dates:
                if 'DPGE101' in dpge_devices_found:
                    all_fpms_dates = all_dpge_dates
                else:
                    raise ValueError("追溯结果中无有效的FPMS时间，无法确定TOPO分析结束日期。")

            earliest_dpge_date = min(all_dpge_dates); latest_fpms_date = max(all_fpms_dates)
            self.topo_start_date.set_date(earliest_dpge_date.date()); self.topo_end_date.set_date(latest_fpms_date.date())
            
            fpms_devices_to_select = set(row[8].strip() for row in self.trace_results if row[8])
            
            if 'DPGE003' in dpge_devices_found: fpms_devices_to_select.add('FPMS004')
            if 'DPGE004' in dpge_devices_found: fpms_devices_to_select.add('FPMS007')
            if 'DPGE101' in dpge_devices_found: fpms_devices_to_select.add('DPGE101')
            
            if not fpms_devices_to_select: messagebox.showwarning("无FPMS机台", "追溯结果中未找到相关的FPMS机台。TOPO机台列表将保持默认。")
            else:
                self.topo_device_listbox.selection_clear(0, tk.END); all_topo_devices = self.topo_device_listbox.get(0, tk.END); found_any = False
                for device in sorted(list(fpms_devices_to_select)):
                    if device in all_topo_devices: idx = all_topo_devices.index(device); self.topo_device_listbox.selection_set(idx); found_any = True
                if not found_any: messagebox.showwarning("机台不匹配", "追溯到的FPMS机台均不在TOPO列表中，请手动选择。"); self.topo_device_listbox.selection_set(0, tk.END)
            self.topo_run_button.config(state=tk.NORMAL); self.app.update_progress("参数已成功填充，请确认后开始TOPO分析。", None, 'auto_topo'); messagebox.showinfo("成功", "追溯结果已成功填充至TOPO参数区域。")
        except Exception as e: messagebox.showerror("填充失败", f"处理追溯结果并填充时出错: {e}"); self.topo_run_button.config(state=tk.DISABLED)
    
    def _start_thk_save_and_calc_thread(self):
        if not self.trace_results:
            messagebox.showerror("错误", "没有可供处理的追溯结果。")
            return

        self.log_messages = []
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_base_dir = os.path.join(os.getcwd(), f"THK_SECTOR_OUTPUT_{timestamp}")
        
        def save_and_calc_logic_wrapper():
            self.app.update_progress("正在开始保存 THK Sector 文件...", 0, 'auto_trace')
            
            self._run_thk_sector_copy_logic()
            
            if self.app.stop_event.is_set():
                self.log_messages.append("\n操作被用户取消。")
                self.app.update_progress("已取消", 100, 'auto_trace')
                return

            self.app.update_progress("文件复制完成，开始计算...", 50, 'auto_trace')
            self._run_thk_calculation_logic()

            self.app.update_progress("计算完成。", 100, 'auto_trace')
            
            summary_message = f"THK Sector 文件保存和计算完成。\n\n"
            summary_message += f"结果保存在: {self.output_base_dir}\n"

            if self.log_messages:
                log_file_path = os.path.join(self.output_base_dir, "_log.txt")
                try:
                    with open(log_file_path, 'w', encoding='utf-8') as f:
                        f.write("\n".join(self.log_messages))
                    summary_message += f"\n详细日志已保存到: {log_file_path}"
                except Exception as e:
                    summary_message += f"\n无法写入日志文件: {e}"
            
            messagebox.showinfo("处理完成", summary_message)

        self.app.start_thread(save_and_calc_logic_wrapper, self._set_trace_controls_state)

    def _run_thk_sector_copy_logic(self):
        if not self.trace_results:
            return

        os.makedirs(self.output_base_dir, exist_ok=True)
        
        self.log_messages.append(f"--- THK Sector 文件保存日志 (V11 - 回退查找) ---")
        self.log_messages.append(f"输出文件夹: {self.output_base_dir}")
        self.log_messages.append(f"总共 {len(self.trace_results)} 个 Sublots 被处理。")
        self.log_messages.append("-" * 30)

        total = len(self.trace_results)
        copy_success_count = 0
        copy_error_count = 0

        for i, row in enumerate(self.trace_results):
            if self.app.stop_event.is_set():
                self.log_messages.append("操作被用户取消。")
                break
            
            self.app.update_progress(f"正在复制 {i+1}/{total}...", (i+1)/total * 50, 'auto_trace')

            sublot_id = ""
            try:
                prod_id, sublot_id, dpol_eqp, dpol_time, dpge_eqp, dpge_time, fpol_eqp, fpol_time, fpms_eqp, fpms_time = row
                
                if not sublot_id:
                    self.log_messages.append(f"跳过: 第 {i+1} 行缺少 Sublot ID。")
                    copy_error_count += 1
                    continue

                sublot_id = sublot_id.strip()
                sublot_dest_folder = os.path.join(self.output_base_dir, sublot_id)
                os.makedirs(sublot_dest_folder, exist_ok=True)


                dpge_eqp_id = dpge_eqp.strip() if dpge_eqp else None
                fpms_eqp_id = fpms_eqp.strip() if fpms_eqp else None

                dp_info = self._find_primary_sector_file(
                    eqp_type='DPGE', eqp_id=dpge_eqp_id, eqp_time=dpge_time, sublot_id=sublot_id
                )
                fp_info = self._find_primary_sector_file(
                    eqp_type='FPMS', eqp_id=fpms_eqp_id, eqp_time=fpms_time, sublot_id=sublot_id
                )

                dp_path = dp_info['source_path'] if dp_info else None
                fp_path = fp_info['source_path'] if fp_info else None
                
                original_dp_name = dp_info['original_filename'] if dp_info else None
                original_fp_name = fp_info['original_filename'] if fp_info else None

                if dp_path and not fp_path and fpms_eqp_id and original_dp_name:
                    self.log_messages.append(f"[查找] {sublot_id}: 未在主路径找到FPMS文件。正在回退查找...")
                    fp_path = self._search_fallback_paths(fpms_eqp_id, original_dp_name)
                    if fp_path:
                        self.log_messages.append(f"[回退成功] {sublot_id}: 在回退路径中找到 {fpms_eqp_id} 的文件。")

                elif fp_path and not dp_path and dpge_eqp_id and original_fp_name:
                    self.log_messages.append(f"[查找] {sublot_id}: 未在主路径找到DPGE文件。正在回退查找...")
                    mapped_dpge_id = self._get_mapped_dpge_id(dpge_eqp_id) 
                    dp_path = self._search_fallback_paths(mapped_dpge_id, original_fp_name)
                    if dp_path:
                        self.log_messages.append(f"[回退成功] {sublot_id}: 在回退路径中找到 {dpge_eqp_id}({mapped_dpge_id}) 的文件。")

                dp_copied = False
                fp_copied = False
                
                if dp_path:
                    try:
                        dest_dp = os.path.join(sublot_dest_folder, f"DP_{os.path.basename(dp_path)}")
                        if not os.path.exists(dest_dp):
                            shutil.copy(dp_path, dest_dp)
                        dp_copied = True
                    except Exception as copy_e:
                        self.log_messages.append(f"[复制错误] DP {sublot_id}: {copy_e}")
                
                if fp_path:
                    try:
                        dest_fp = os.path.join(sublot_dest_folder, f"FP_{os.path.basename(fp_path)}")
                        if not os.path.exists(dest_fp):
                            shutil.copy(fp_path, dest_fp)
                        fp_copied = True
                    except Exception as copy_e:
                        self.log_messages.append(f"[复制错误] FP {sublot_id}: {copy_e}")

                if dp_copied:
                    copy_success_count += 1
                    self.log_messages.append(f"[复制成功] DPGE: Sublot {sublot_id} / EQP {dpge_eqp_id}")
                else:
                    copy_error_count += 1
                    self.log_messages.append(f"[复制失败] DPGE 文件未找到: Sublot {sublot_id} / EQP {dpge_eqp_id}")

                if fp_copied:
                    copy_success_count += 1
                    self.log_messages.append(f"[复制成功] FPMS: Sublot {sublot_id} / EQP {fpms_eqp_id}")
                else:
                    copy_error_count += 1
                    self.log_messages.append(f"[复制失败] FPMS 文件未找到: Sublot {sublot_id} / EQP {fpms_eqp_id}")

            except Exception as e:
                self.log_messages.append(f"[复制错误] 处理 Sublot {sublot_id} 时发生意外: {e}")
                copy_error_count += 1
        
        self.log_messages.append("-" * 30)
        self.log_messages.append(f"文件复制小计: 成功 {copy_success_count} / 失败或跳过 {copy_error_count}")
        self.log_messages.append("\n" + "=" * 30 + "\n")


    def _search_fallback_paths(self, device_name: str, original_filename: str) -> Optional[str]:
        if not device_name or not original_filename:
            return None
            
        search_paths = [
            Config.ERO_PRE_PATH_TEMPLATE,
            Config.ERO_POST_PATH_TEMPLATE,
            Config.ERO_ERROR_PATH_TEMPLATE,
            Config.THK_PROFILE_PATH_TEMPLATE
        ]
        
        for path_template in search_paths:
            try:
                if "{device}" not in path_template:
                    search_dir = path_template
                else:
                    search_dir = path_template.format(device=device_name)
                
                potential_path = os.path.join(search_dir, original_filename)
                
                if os.path.exists(potential_path):
                    return potential_path
            except Exception as e:
                print(f"Error searching fallback path {path_template}: {e}")
                continue
                
        return None

    def _run_thk_calculation_logic(self):
        self.log_messages.append(f"--- THK Sector 文件计算日志 (V14 - 恢复V12计算) ---")
        import gc
        
        try:
            def read_jagged_csv_to_dataframe(filepath):
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    reader = csv.reader(f)
                    all_data_list = list(reader)
                return pd.DataFrame(all_data_list)

            sublot_folders = [f.path for f in os.scandir(self.output_base_dir) if f.is_dir()]
            total = len(sublot_folders)
            calc_success_count = 0
            calc_error_count = 0
            
            for i, sublot_folder in enumerate(sublot_folders):
                if self.app.stop_event.is_set():
                    self.log_messages.append("操作被用户取消。")
                    break
                
                sublot_id = os.path.basename(sublot_folder)
                self.app.update_progress(f"正在计算 {sublot_id} ({i+1}/{total})...", 50 + (i+1)/total * 50, 'auto_trace')

                fig_dir = os.path.join(sublot_folder, "REMOVAL_Fig")
                os.makedirs(fig_dir, exist_ok=True)

                try:
                    dp_files = glob.glob(os.path.join(sublot_folder, "DP_*.csv"))
                    fp_files = glob.glob(os.path.join(sublot_folder, "FP_*.csv"))

                    if not dp_files or not fp_files:
                        self.log_messages.append(f"[计算失败] {sublot_id}: 缺少 DP_ 或 FP_ 文件。")
                        calc_error_count += 1
                        continue
                    
                    dp_file_path = dp_files[0]
                    fp_file_path = fp_files[0]

                    calc_file_path = os.path.join(sublot_folder, f"REMOVAL_{sublot_id}.csv") 

                    df_dp_full = read_jagged_csv_to_dataframe(dp_file_path)
                    df_fp_full = read_jagged_csv_to_dataframe(fp_file_path)

                    START_ROW_H = 25
                    WAFER_ID_COL = 2
                    START_COL_J = 8 
                    END_COL_K = 756 

                    dp_data_rows = df_dp_full.iloc[START_ROW_H:].copy()
                    if dp_data_rows.empty:
                        self.log_messages.append(f"[计算失败] {sublot_id}: DP_ 文件在第26行后没有数据。")
                        calc_error_count += 1
                        continue
                        
                    dp_wafer_ids = dp_data_rows.iloc[:, WAFER_ID_COL]
                    dp_numeric = dp_data_rows.iloc[:, START_COL_J : END_COL_K + 1].apply(pd.to_numeric, errors='coerce')
                    dp_numeric.index = dp_wafer_ids 

                    fp_data_rows = df_fp_full.iloc[START_ROW_H:].copy()
                    if fp_data_rows.empty:
                        self.log_messages.append(f"[计算失败] {sublot_id}: FP_ 文件在第26行后没有数据。")
                        calc_error_count += 1
                        continue
                        
                    fp_wafer_ids = fp_data_rows.iloc[:, WAFER_ID_COL]
                    fp_numeric = fp_data_rows.iloc[:, START_COL_J : END_COL_K + 1].apply(pd.to_numeric, errors='coerce')
                    fp_numeric.index = fp_wafer_ids

                    df_result = dp_numeric.subtract(fp_numeric, fill_value=np.nan)

                    df_result.to_csv(calc_file_path, header=False, index=True, encoding='utf-8-sig')

                    num_cols = df_result.shape[1]
                    x_values = [j * 0.2 for j in range(num_cols)]
                    
                    plot_count = 0

                    fig_overlay, ax_overlay = plt.subplots(figsize=(12, 6))
                    
                    for wafer_id, row_series in df_result.iterrows():
                        
                        safe_wafer_id_str = str(wafer_id).strip()
                        if pd.isna(wafer_id) or safe_wafer_id_str == "":
                            continue
                            
                        y_values = row_series.values

                        if np.all(pd.isna(y_values)):
                            self.log_messages.append(f"[绘图跳过] {sublot_id} / {wafer_id}: 仅有Wafer ID，无数据 (All NaN)。")
                            continue
                        
                        non_nan_values = y_values[~pd.isna(y_values)] 
                        
                        if non_nan_values.size == 0:
                            continue 
                            
                        if np.all(np.abs(non_nan_values) < 1e-6):
                            self.log_messages.append(f"[绘图跳过] {sublot_id} / {wafer_id}: 数据全为0，跳过绘图。")
                            continue

                        fig_individual, ax_individual = plt.subplots(figsize=(12, 6))
                        try:
                            ax_individual.plot(x_values, y_values) 
                            ax_individual.set_title(f"Removal Result - {safe_wafer_id_str}") 
                            ax_individual.set_xlabel("Position (mm)")
                            ax_individual.set_ylabel("Removal (um)") 
                            ax_individual.grid(True)
                            
                            safe_filename = "".join(c for c in safe_wafer_id_str if c.isalnum() or c in ('-','_','.'))
                            if not safe_filename:
                                safe_filename = f"plot_{plot_count}"
                                
                            plot_filename = os.path.join(fig_dir, f"{safe_filename}.png")
                            plt.tight_layout()
                            fig_individual.savefig(plot_filename, dpi=150)
                            
                            plot_count += 1
                        except Exception as plot_e:
                            self.log_messages.append(f"[绘图警告] {sublot_id} / {wafer_id}: 绘图失败: {plot_e}")
                        finally:
                            plt.close(fig_individual)
                        ax_overlay.plot(x_values, y_values, label=safe_wafer_id_str, alpha=0.5)

                    try:
                        ax_overlay.set_title(f"Removal Overlay Plot - {sublot_id}") 
                        ax_overlay.set_xlabel("Position (mm)")
                        ax_overlay.set_ylabel("Removal (um)")
                        ax_overlay.grid(True)
                        
                        overlay_filename = os.path.join(fig_dir, f"_OVERLAY_REMOVAL_{sublot_id}.png")
                        plt.tight_layout()
                        fig_overlay.savefig(overlay_filename, dpi=200, bbox_inches='tight')
                        
                    except Exception as overlay_e:
                         self.log_messages.append(f"[叠图警告] {sublot_id}: 叠图保存失败: {overlay_e}")
                    finally:
                        plt.close(fig_overlay)

                    plt.close('all')
                    gc.collect()
                    
                    self.log_messages.append(f"[绘图成功] {sublot_id}: 已在 '{os.path.basename(fig_dir)}' 中保存 {plot_count} 张图 + 1 张叠图。")
                    self.log_messages.append(f"[计算成功] {sublot_id}: 已保存 {calc_file_path} (已按Wafer ID对齐)")
                    calc_success_count += 1
                    
                except Exception as e:
                    self.log_messages.append(f"[计算失败] {sublot_id}: 发生错误: {e}")
                    import traceback
                    self.log_messages.append(traceback.format_exc())
                    calc_error_count += 1
                    
        except Exception as e:
            self.log_messages.append(f"[计算错误] 无法遍历文件夹: {e}")
        
        self.log_messages.append("-" * 30)
        self.log_messages.append(f"文件计算小计: 成功 {calc_success_count} / 失败 {calc_error_count}")

        if sublot_folders:
            try:
                first_sublot_fig_dir = os.path.join(sublot_folders[0], "REMOVAL_Fig")
                os.makedirs(first_sublot_fig_dir, exist_ok=True)
                log_file_path_fig = os.path.join(first_sublot_fig_dir, "_log.txt")
                
                with open(log_file_path_fig, 'w', encoding='utf-8') as f:
                    f.write("\n".join(self.log_messages))
                log_file_path_root = os.path.join(self.output_base_dir, "_log.txt")
                with open(log_file_path_root, 'w', encoding='utf-8') as f:
                    f.write("\n".join(self.log_messages))
                    
            except Exception as log_e:
                print(f"写入日志失败: {log_e}")

    def _start_topo_thread(self):
        self.app.start_thread(self._run_topo_logic, self._set_topo_controls_state)
    
    def _run_topo_logic(self):
        start_date = self.topo_start_date.get_date()
        end_date = self.topo_end_date.get_date()
        devices = [self.topo_device_listbox.get(i) for i in self.topo_device_listbox.curselection()]
        
        raw_lot_input = self.topo_lot_id_entry.get()
        lot_prefixes = [p.strip() for p in raw_lot_input.replace('\n', ',').replace('\r', '').split(',') if p.strip()]
        
        export_profile = self.topo_export_profile_var.get()
        fpms007_as_dp = self.topo_fpms007_dp_var.get()
        process_both = self.topo_process_both_var.get()

        custom_prefix = ""
        is_target_product_run = False
        target_products = {'PPCS90A006-A2', 'PPCS90A007-A2'}

        automation_instance = self.app.automation if self.app.current_function == 'Product自动化处理' else self.app.sublot_automation
        
        current_run_products = set()
        if hasattr(automation_instance, 'trace_results') and automation_instance.trace_results:
             current_run_products.update(row[0].strip() for row in automation_instance.trace_results if row[0])
        elif hasattr(automation_instance, 'trace_prod_id_combo'):
             current_run_products.add(automation_instance.trace_prod_id_combo.get())
        
        is_target_product_run = any(p in target_products for p in current_run_products)

        if isinstance(automation_instance, SublotAutomationFunction):
            if automation_instance.search_prefix:
                raw_prefix = automation_instance.search_prefix
                sublot_list = [s.strip() for s in re.split(r'[\n\r\s,]+', raw_prefix) if s.strip()]
                
                if len(sublot_list) > 2:
                    clean_prefix = f"{sublot_list[0]}-{sublot_list[1]}_etc"
                else:
                    clean_prefix = "-".join(sublot_list)

                if len(clean_prefix) > 50:
                    clean_prefix = f"{clean_prefix[:50]}_etc"
                    
                custom_prefix = f"{clean_prefix}_"
                
        elif isinstance(automation_instance, AutomationFunction):
            selected_prod_id = automation_instance.trace_prod_id_combo.get()
            selected_fpol_devices = [automation_instance.trace_eqp_listbox.get(i) for i in automation_instance.trace_eqp_listbox.curselection()]
            
            fpol_str = ""
            if selected_fpol_devices:
                if len(selected_fpol_devices) > 3: 
                    fpol_str = f"MultiDevices({len(selected_fpol_devices)})"
                else:
                    fpol_str = "-".join(sorted(selected_fpol_devices))
            
            if fpol_str:
                if selected_prod_id != 'ALL':
                    custom_prefix = f"{fpol_str}_{selected_prod_id}_"
                else:
                    custom_prefix = f"{fpol_str}_ALL_"

        if not devices or not lot_prefixes:
            messagebox.showerror("参数缺失", "请确保机台和Lot ID已正确填充。")
            return

        output_folders = []

        if process_both:
            result_folder_ee1, data_ee1, headers = None, None, None
            result_folder_ee2, data_ee2, _ = None, None, None
            
            self.app.update_progress("自动化: 开始处理 EE2 (收集数据)...", 0, 'auto_topo')
            result_folder_ee2, data_ee2, _ = self.app.topo_data.execute_topo_processing(
                start_date=start_date, end_date=end_date, devices=devices, lot_prefixes=lot_prefixes,
                file_prefix="IMP_W26D08_D35S72_EE2", export_profile=export_profile, fpms007_as_dp=fpms007_as_dp,
                output_suffix="_EE2", custom_prefix=custom_prefix
            )
            if result_folder_ee2: output_folders.append(result_folder_ee2)
            if self.app.stop_event.is_set(): return

            self.app.update_progress("自动化: 开始处理 EE1 (收集数据)...", 50, 'auto_topo')
            result_folder_ee1, data_ee1, headers = self.app.topo_data.execute_topo_processing(
                start_date=start_date, end_date=end_date, devices=devices, lot_prefixes=lot_prefixes,
                file_prefix="IMP_W26D08_D35S72_EE1", export_profile=export_profile, fpms007_as_dp=fpms007_as_dp,
                output_suffix="_EE1", custom_prefix=custom_prefix
            )
            if result_folder_ee1: output_folders.append(result_folder_ee1)
            if self.app.stop_event.is_set(): return

            if is_target_product_run:
                self._perform_esfqr_replacement(data_ee1, data_ee2, headers, result_folder_ee2)
        else:
            prefix = self.topo_file_prefix_combo.get()
            output_suffix = "_EE2" if "EE2" in prefix else "_EE1"
            self.app.update_progress(f"自动化: 开始处理 {output_suffix}...", 0, 'auto_topo')

            result_folder, _, _ = self.app.topo_data.execute_topo_processing(
                start_date=start_date, end_date=end_date, devices=devices, lot_prefixes=lot_prefixes,
                file_prefix=prefix, export_profile=export_profile, fpms007_as_dp=fpms007_as_dp,
                output_suffix=output_suffix, custom_prefix=custom_prefix
            )
            if result_folder:
                output_folders.append(result_folder)

        if not self.app.stop_event.is_set() and output_folders:
            self.app.update_progress("自动化流程完成！", 100, 'auto_topo')
            message = "TOPO分析完成！结果已保存在以下文件夹中:\n\n" + "\n".join(output_folders)
            self.app.root.after(0, messagebox.showinfo, "成功", message)
        elif self.app.stop_event.is_set():
             self.app.update_progress("自动化流程已取消。", 100, 'auto_topo')

    def _perform_esfqr_replacement(self, data_ee1, data_ee2, headers, result_folder_ee2):
        self.app.root.after(0, messagebox.showinfo, "Debug", "进入ESFQR替换函数。")

        if not (data_ee1 and data_ee2 and headers and result_folder_ee2):
            missing = []
            if not data_ee1: missing.append("- 未能成功生成或找到EE1的数据。")
            if not data_ee2: missing.append("- 未能成功生成或找到EE2的数据。")
            if not headers: missing.append("- 未能从EE1的结果中获取列标题。")
            if not result_folder_ee2: missing.append("- 未找到EE2的输出文件夹路径。")
            
            self.app.root.after(0, messagebox.showwarning, "跳过替换操作", "无法执行ESFQR替换，因为：\n" + "\n".join(missing))
            return

        self.app.update_progress("自动化: 正在生成替换ESFQR值的EE2文件...", 90, 'auto_topo')
        try:
            esfqr_col_index = 13
            wafer_id_col_index = 3
            device_col_index = 1
            
            ee1_esfqr_map = {
                (row[wafer_id_col_index].strip(), row[device_col_index].strip()): row[esfqr_col_index] 
                for row in data_ee1
            }

            modified_data_ee2 = []
            replacement_count = 0
            for i, row_ee2 in enumerate(data_ee2):
                new_row = list(row_ee2)
                wafer_id = new_row[wafer_id_col_index].strip()
                device = new_row[device_col_index].strip()
                composite_key = (wafer_id, device)
                
                if composite_key in ee1_esfqr_map:
                    new_row[esfqr_col_index] = ee1_esfqr_map[composite_key]
                    replacement_count += 1
                    
                modified_data_ee2.append(new_row)
            
            base_name = os.path.basename(result_folder_ee2)
            new_filename = f"{base_name}_ESFQR_Replaced.csv"
            new_filepath = os.path.join(result_folder_ee2, new_filename)
            
            FileProcessor.write_custom_csv(new_filepath, modified_data_ee2, headers)
            
        except Exception as e:
            self.app.root.after(0, messagebox.showerror, "文件写入错误", f"无法写入替换后的ESFQR文件: {e}")

    def _set_trace_controls_state(self, enable: bool):
        state = tk.NORMAL if enable else tk.DISABLED
        self.trace_run_button.config(state=state)
        
        if enable:
            has_results = bool(self.trace_results)
            self.transfer_button.config(state=tk.NORMAL if has_results else tk.DISABLED)
            self.save_thk_button.config(state=tk.NORMAL if has_results else tk.DISABLED)
            self.export_button.config(state=tk.NORMAL if has_results else tk.DISABLED)
            self.app.update_progress("准备就绪", None, 'auto_trace')
        else:
            self.transfer_button.config(state=tk.DISABLED)
            self.save_thk_button.config(state=tk.DISABLED)
            self.export_button.config(state=tk.DISABLED)
    
    def _set_topo_controls_state(self, enable: bool):
        state = tk.NORMAL if enable else tk.DISABLED
        self.topo_run_button.config(state=state)
        if enable:
            self.app.update_progress("准备就绪", None, 'auto_topo')


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
    export_trace_results_to_csv = AutomationFunction.export_trace_results_to_csv