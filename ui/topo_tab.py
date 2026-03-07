import os
import csv
import glob
import gc
import re
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Checkbutton
from tkcalendar import DateEntry
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any

import matplotlib
matplotlib.use('Agg') # Use Agg backend for non-interactive plotting
import matplotlib.pyplot as plt
import numpy as np

from config import Config
from data_processor import FileProcessor

class TopoDataFunction:
    """Implements the TOPO DATA feature."""
    def __init__(self, app):
        self.app = app
        self.frame = None
        self.fpms007_dp_var = tk.BooleanVar(value=True)
    
    def show(self):
        """Shows the TOPO DATA feature UI."""
        if self.frame: self.frame.destroy()
        self.frame = ttk.Frame(self.app.right_frame)
        self.frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(self.frame, text="TOPO DATA 分析", font=('Arial', 14, 'bold')).grid(row=0, column=0, columnspan=4, pady=10)
        
        date_frame = ttk.Frame(self.frame); date_frame.grid(row=1, column=0, columnspan=4, pady=5, sticky="ew")
        ttk.Label(date_frame, text="开始日期:").pack(side=tk.LEFT, padx=5)
        self.topo_start_date = DateEntry(date_frame, date_pattern='yyyy-mm-dd', width=12); self.topo_start_date.pack(side=tk.LEFT)
        ttk.Label(date_frame, text="结束日期:").pack(side=tk.LEFT, padx=(20, 5))
        self.topo_end_date = DateEntry(date_frame, date_pattern='yyyy-mm-dd', width=12); self.topo_end_date.pack(side=tk.LEFT)

        device_ui_frame = ttk.LabelFrame(self.frame, text="FPMS机台"); device_ui_frame.grid(row=2, column=0, columnspan=4, pady=5, sticky="ew")
        
        listbox_frame = ttk.Frame(device_ui_frame)
        listbox_frame.pack(side=tk.LEFT, padx=5, pady=5, fill=tk.BOTH, expand=True)

        self.device_listbox = tk.Listbox(listbox_frame, selectmode=tk.MULTIPLE, exportselection=0, height=5)
        self.device_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(listbox_frame, orient=tk.VERTICAL, command=self.device_listbox.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.device_listbox.config(yscrollcommand=scrollbar.set)

        # --- 修改: 列表中加入 DPGE101 ---
        device_list = [f"FPMS{num:03d}" for num in range(1, 13)]
        device_list.append("DPGE101") # 新增 DPGE101
        
        for device in device_list: 
            self.device_listbox.insert(tk.END, device)
            
        self.device_listbox.selection_set(0, tk.END)

        listbox_button_frame = ttk.Frame(device_ui_frame)
        listbox_button_frame.pack(side=tk.LEFT, padx=5, fill=tk.Y)
        ttk.Button(listbox_button_frame, text="全选", command=lambda: self.device_listbox.selection_set(0, tk.END)).pack(pady=2, fill=tk.X)
        ttk.Button(listbox_button_frame, text="取消全选", command=lambda: self.device_listbox.selection_clear(0, tk.END)).pack(pady=2, fill=tk.X)
        
        input_frame = ttk.Frame(self.frame); input_frame.grid(row=3, column=0, columnspan=4, pady=5, sticky="ew")
        ttk.Label(input_frame, text="Lot ID (逗号分隔):").grid(row=0, column=0, sticky=tk.W)
        self.subfolder_prefix_entry = ttk.Entry(input_frame, width=40); self.subfolder_prefix_entry.grid(row=0, column=1, sticky=tk.W)
        ttk.Label(input_frame, text="文件前缀:").grid(row=1, column=0, sticky=tk.W)
        self.file_prefix_dropdown = ttk.Combobox(input_frame, values=Config.IMP_PREFIXES, state="readonly", width=38); self.file_prefix_dropdown.grid(row=1, column=1, sticky=tk.W); self.file_prefix_dropdown.current(1)

        options_frame = ttk.Frame(self.frame); options_frame.grid(row=4, column=0, columnspan=4, pady=5)
        self.thickness_profile_var = tk.BooleanVar(value=True)
        Checkbutton(options_frame, text="导出Thickness Profile", variable=self.thickness_profile_var).pack(side=tk.LEFT, padx=5)
        Checkbutton(options_frame, text="FPMS007为DP测试", variable=self.fpms007_dp_var).pack(side=tk.LEFT, padx=5)
        
        self.process_both_var = tk.BooleanVar(value=False)
        Checkbutton(options_frame, text="自动处理 EE1 & EE2 (先EE2)", variable=self.process_both_var).pack(side=tk.LEFT, padx=5)

        progress_frame = ttk.Frame(self.frame); progress_frame.grid(row=5, column=0, columnspan=4, pady=10, sticky="ew")
        self.topo_progress_label = ttk.Label(progress_frame, text="准备就绪"); self.topo_progress_label.pack(fill=tk.X)
        self.topo_progress = ttk.Progressbar(progress_frame, orient=tk.HORIZONTAL, mode='determinate'); self.topo_progress.pack(fill=tk.X, expand=True)
        
        button_frame = ttk.Frame(self.frame); button_frame.grid(row=6, column=0, columnspan=4, pady=10)
        self.topo_read_button = ttk.Button(button_frame, text="读取文件", command=self.start_topo_processing_thread); self.topo_read_button.pack(side=tk.LEFT, padx=5)
        self.topo_cancel_button = ttk.Button(button_frame, text="取消", command=self.on_topo_cancel, state=tk.DISABLED); self.topo_cancel_button.pack(side=tk.LEFT, padx=5)

    def _get_base_folder_path(self, current_date: datetime.date, device_name: str) -> str:
        """Determines the correct base folder path based on the date and device."""
        
        # --- 修改: 处理 DPGE101 的特殊路径 ---
        if device_name == "DPGE101":
            return os.path.join(Config.DPGE101_BASE_PATH, current_date.strftime('%Y%m%d'))

        # 原有逻辑
        if current_date >= Config.PATH_TRANSITION_DATE:
            # 关键修复：处理 NEW_BASE_PATH 是列表的情况 (解决 TypeError: expected str... not list)
            base = Config.NEW_BASE_PATH
            if isinstance(base, list):
                # 假设列表的第一个元素是主要的 FPMS 路径
                base = base[0] 
            
            # 确保 base 是字符串后再进行 join
            return os.path.join(str(base), f"01_{device_name}", "01_Production", current_date.strftime('%Y%m%d'))
        else:
            return os.path.join(Config.OLD_BASE_PATH, f"02_{device_name}", "01_Production", current_date.strftime('%Y%m%d'))

    def start_topo_processing_thread(self):
        """Starts file processing in a separate thread."""
        self.app.start_thread(self._process_topo_data_from_ui, self._set_controls_state)

    def _process_topo_data_from_ui(self):
        """Gathers parameters from the UI and starts TOPO processing."""
        inputs = self._gather_ui_inputs()
        if not inputs: return

        if self.process_both_var.get():
            prefixes_to_run = ["IMP_W26D08_D35S72_EE2", "IMP_W26D08_D35S72_EE1"]
        else:
            prefixes_to_run = [inputs['file_prefix']]
        
        output_folders = []
        for prefix in prefixes_to_run:
            if self.app.stop_event.is_set():
                break

            output_suffix = "_EE2" if "EE2" in prefix else "_EE1"
            self.app.update_progress(f"正在开始处理 {output_suffix}...", 0, 'topo')
            
            result_folder, _, _ = self.execute_topo_processing(
                start_date=inputs['start_date'],
                end_date=inputs['end_date'],
                devices=inputs['devices'],
                lot_prefixes=inputs['lot_prefixes'],
                file_prefix=prefix,
                export_profile=inputs['export_profile'],
                fpms007_as_dp=inputs['fpms007_as_dp'],
                output_suffix=output_suffix
            )
            if result_folder:
                output_folders.append(result_folder)

        if not self.app.stop_event.is_set() and output_folders:
            message = "所有处理任务完成！结果已保存在以下文件夹中:\n\n" + "\n".join(output_folders)
            messagebox.showinfo("成功", message)
        elif self.app.stop_event.is_set():
            messagebox.showinfo("信息", "处理已由用户取消。")

    def execute_topo_processing(self, start_date, end_date, devices, lot_prefixes, file_prefix, export_profile, fpms007_as_dp, output_suffix: str = "", custom_prefix: str = "", sublot_eqp_map: dict = None) -> Tuple[Optional[str], Optional[List], Optional[List]]:
        import time
        start_time_perf = time.time()
        
        # ⭐ 升级统计器：细分近距(±1s)和远距(±10s)盲狙
        search_stats = {
            'local': [], 'imp_single': [], 'thk_single': [], 
            'multi_tier1': [], 'multi_tier2': [], 
            'fallback': [], 'failed': []
        }

        timestamp = datetime.now().strftime('%H%M%S')
        
        if custom_prefix:
            import re
            custom_prefix = re.sub(r'[<>:"/\\|?*\n\r]', '_', custom_prefix)
        
        folder_name_base = f"{custom_prefix}TOPO_DATA_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        output_folder = f"{folder_name_base}_{timestamp}{output_suffix}"
        
        os.makedirs(output_folder, exist_ok=True)
        os.makedirs(os.path.join(output_folder, "Fig"), exist_ok=True)

        output_data = []
        total_days = (end_date - start_date).days + 1
        
        date_range = [start_date + timedelta(days=x) for x in range(total_days)]
        
        headers = [
            "Date", "Device", "Sublot", "Wafer ID", "Source Slot", "Acquisition Time",
            "Mean Thickness (um)", "Center Thickness (um)", "GBIR (um)", "GFLR (um)", "Bow (um)", "Warp (um)",
            "SFQR Max (um)", "SFQRP99(um)", "ESFQR Max (um)", "ZDD (nm/mm^2)", "THA 2mm (nm)", "THA 10mm (nm)","THA 2mm 0 %(nm)", "THA 10mm 0 %(nm)", 
            "ERO147", "ERO148", "ERO149", "MaxR", "MaxE",
            "Convexity", "Edge", "Center_Slope", "Mid_Slope"
        ]
        
        for i, current_date in enumerate(date_range):
            if self.app.stop_event.is_set(): break
            
            progress = (i + 1) / total_days * 100
            self.app.update_progress(f"正在处理日期 {current_date.strftime('%Y%m%d')} (批次: {output_suffix})...", progress, 'topo')
            
            for device_name in devices:
                if self.app.stop_event.is_set(): break
                self._process_device_for_date(current_date, device_name, lot_prefixes, file_prefix, export_profile, fpms007_as_dp, output_folder, output_data, sublot_eqp_map, search_stats)

        if not self.app.stop_event.is_set() and output_data:
            self._write_output_csv(output_folder, output_data, headers)
            self.app.update_progress(f"{output_suffix} 处理完成！结果已保存。", 100, 'topo')
            
            # =====================================================================
            # ⭐ 生成执行情况汇总日志
            # =====================================================================
            try:
                end_time_perf = time.time()
                total_seconds = end_time_perf - start_time_perf
                
                total_wafers = len(output_data)
                unique_sublots = set(row[2] for row in output_data if len(row) > 2 and row[2])
                total_sublots = len(unique_sublots)
                
                m, s = divmod(total_seconds, 60)
                h, m = divmod(m, 60)
                time_str = f"{int(h)}小时 {int(m)}分钟 {s:.2f}秒" if h > 0 else f"{int(m)}分钟 {s:.2f}秒"
                
                log_filename = os.path.join(output_folder, f"{os.path.basename(output_folder)}_RunLog.log")
                with open(log_filename, 'w', encoding='utf-8') as log_f:
                    log_f.write("=" * 70 + "\n")
                    log_f.write("                 TOPO DATA 数据处理执行日志                 \n")
                    log_f.write("=" * 70 + "\n\n")
                    log_f.write(f"任务名称:      {os.path.basename(output_folder)}\n")
                    log_f.write(f"执行时间:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    
                    log_f.write(f"【查询条件】\n")
                    log_f.write(f"- 时间范围:    {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}\n")
                    log_f.write(f"- 数据后缀:    {output_suffix}\n\n")
                    
                    log_f.write(f"【处理统计】\n")
                    log_f.write(f"- Sublot 数量: {total_sublots} 个\n")
                    log_f.write(f"- Wafer 总数:  {total_wafers} 片\n")
                    log_f.write(f"- 总计耗时:    {time_str}\n\n")

                    log_f.write(f"【查找策略性能与明细分析】\n")
                    
                    def format_stat_list(label, key, show_sublots=True, is_exception_type=False):
                        lst = search_stats.get(key, [])
                        count = len(lst)
                        log_f.write(f"- {label.ljust(14)}: {str(count).rjust(4)} 次\n")
                        
                        if count > 0 and show_sublots:
                            if not is_exception_type:
                                import textwrap
                                wrapped = textwrap.fill(", ".join(lst), width=75, initial_indent="    └─ 涉及: ", subsequent_indent="             ")
                                log_f.write(f"{wrapped}\n")
                            else:
                                for item in lst:
                                    sublot_str, trace_logs = item 
                                    log_f.write(f"    └─ 涉及 Sublot: {sublot_str}\n")
                                    log_f.write(f"       [详细追溯诊断]:\n")
                                    for trace_line in trace_logs:
                                        clean_line = trace_line.replace('\n', '').strip()
                                        log_f.write(f"           {clean_line}\n")
                                    log_f.write("\n")
                                    
                    format_stat_list("本地完美匹配", "local", show_sublots=False) 
                    format_stat_list("IMP单发盲狙", "imp_single", show_sublots=True, is_exception_type=False)
                    format_stat_list("THK单发盲狙", "thk_single", show_sublots=True, is_exception_type=False)
                    
                    log_f.write("\n--- 以下为需要关注的降级/异常情况 ---\n")
                    # ⭐ 区分近距和远距
                    format_stat_list("近距盲狙(±1s)", "multi_tier1", show_sublots=True, is_exception_type=True)
                    format_stat_list("远距盲狙(±10s)", "multi_tier2", show_sublots=True, is_exception_type=True)
                    format_stat_list("使用残缺备胎", "fallback", show_sublots=True, is_exception_type=True)
                    format_stat_list("彻底查找失败", "failed", show_sublots=True, is_exception_type=True)
                    log_f.write("\n")
                    
                    log_f.write(f"【涉及的 Sublots 全列表】\n")
                    for sl in sorted(list(unique_sublots)):
                        log_f.write(f"  - {sl}\n")
                        
            except Exception as log_e:
                print(f"Failed to write execution log: {log_e}")
                
            return os.path.abspath(output_folder), output_data, headers

        return None, None, None
    def _gather_ui_inputs(self) -> Optional[Dict[str, Any]]:
        """Gathers and validates all user inputs from the UI."""
        selected_devices = [self.device_listbox.get(i) for i in self.device_listbox.curselection()]
        if not selected_devices:
            messagebox.showwarning("警告", "请至少选择一个设备。")
            return None
            
        return {
            "start_date": self.topo_start_date.get_date(),
            "end_date": self.topo_end_date.get_date(),
            "devices": selected_devices,
            "lot_prefixes": [prefix.strip() for prefix in self.subfolder_prefix_entry.get().strip().split(',') if prefix.strip()],
            "file_prefix": self.file_prefix_dropdown.get(),
            "export_profile": self.thickness_profile_var.get(),
            "fpms007_as_dp": self.fpms007_dp_var.get()
        }

    def _process_device_for_date(self, current_date, device_name, lot_prefixes, file_prefix, export_profile, fpms007_as_dp, output_folder, output_data, sublot_eqp_map=None, search_stats=None):
        """Processes all subfolders for a given device on a specific date."""
        base_folder_path = self._get_base_folder_path(current_date, device_name)
        if not os.path.exists(base_folder_path): return
        
        subfolders = self._find_subfolders(base_folder_path, lot_prefixes)
        for subfolder in subfolders:
            if self.app.stop_event.is_set(): break
            
            if device_name == "DPGE101":
                continue 

            if sublot_eqp_map:
                sublot_name = os.path.basename(subfolder)
                expected_eqp = None
                
                for map_sublot, map_eqp in sublot_eqp_map.items():
                    if sublot_name.startswith(map_sublot):
                        expected_eqp = map_eqp
                        break
                
                is_exempted_dpge = False
                if device_name in ["FPMS004", "FPMS007"]:
                    is_exempted_dpge = True 

                if expected_eqp and expected_eqp != device_name and not is_exempted_dpge:
                    print(f"[极速路由优化] 跳过: {sublot_name} 并不在 {device_name} (7020站点应在 {expected_eqp})")
                    continue

            # 传入 search_stats 统计器
            self._process_subfolder(subfolder, current_date, device_name, file_prefix, export_profile, fpms007_as_dp, output_folder, output_data, search_stats)

    def _process_subfolder(self, subfolder_path, current_date, device_name, file_prefix, export_profile, fpms007_as_dp, output_folder, output_data, search_stats=None):
        """Processes IMP, SQMM, and Thickness files within a single subfolder."""
        try:
            sublot_name = os.path.basename(subfolder_path)
            print(f"=== 开始处理 Subfolder: {sublot_name} ===")
            
            # ⭐ 新增：为当前 Sublot 创建专属的日志黑匣子
            sublot_trace_logs = []
            
            imp_result = self._read_csv_file(subfolder_path, file_prefix)
            if not imp_result:
                return
            imp_data, _ = imp_result

            acq_time_for_search = None
            max_slot_row = None
            max_slot_num = -1

            for row in imp_data:
                try:
                    slot_str = row.get('Source Slot', '-1').strip()
                    slot_num_str = ''.join(filter(str.isdigit, slot_str))
                    if not slot_num_str: continue

                    slot_num = int(slot_num_str)
                    if slot_num > max_slot_num:
                        max_slot_num = slot_num
                        max_slot_row = row
                except (ValueError, TypeError):
                    continue

            if max_slot_row:
                time_str = self._format_acquisition_time(max_slot_row.get('Acquisition Date/Time'))
                if time_str:
                    try:
                        raw_time = max_slot_row.get('Acquisition Date/Time')
                        parsed_dt = None
                        for fmt in ['%m/%d/%y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S']:
                            try:
                                parsed_dt = datetime.strptime(raw_time.strip(), fmt)
                                break
                            except ValueError:
                                continue
                        if parsed_dt:
                            acq_time_for_search = parsed_dt
                    except ValueError:
                        pass

            sqmm_result = self._read_csv_file(subfolder_path, Config.SQMM_PREFIXES[0]) or \
                          self._read_csv_file(subfolder_path, Config.SQMM_PREFIXES[1])
            sqmm_data = sqmm_result[0] if sqmm_result else []

            timestamp_suffix = None
            imp_filename = next((f for f in os.listdir(subfolder_path) if f.startswith(file_prefix) and f.endswith(".csv")), None)
            if imp_filename:
                import re
                match = re.search(r'[-_](\d{6,14})\.csv$', imp_filename, re.IGNORECASE)
                if match:
                    timestamp_suffix = match.group(1)

            expected_wafers = len([r for r in imp_data if r.get('Wafer ID', '').strip()])
            
            thick_filename_local = next((f for f in os.listdir(subfolder_path) if f.startswith(Config.THICKNESS_PREFIX)), None)

            # ⭐ 修改：把专属黑匣子 sublot_trace_logs 传进去
            thick_file_path_to_use = self._find_thickness_file(
                device_name=device_name,
                subfolder_path=subfolder_path,
                fpms007_as_dp=fpms007_as_dp,
                thick_filename_local=thick_filename_local,
                acq_time_for_search=acq_time_for_search,
                timestamp_suffix=timestamp_suffix,
                expected_wafers=expected_wafers,
                search_stats=search_stats,
                sublot_trace_logs=sublot_trace_logs
            )

            all_profile_data, all_zeroed_data = [], []
            
            for index, imp_row in enumerate(imp_data):
                wafer_id = imp_row.get('Wafer ID')
                if not wafer_id: continue

                sqmm_row = next((row for row in sqmm_data if row.get('Wafer ID') == wafer_id), {})
                
                thick_metrics, profile, zeroed = (None, None, None)
                if thick_file_path_to_use:
                    thick_metrics, profile, zeroed = FileProcessor.topo_read_thick_file(
                        thick_file_path_to_use, 
                        wafer_id, 
                        export_profile
                    )
                
                if profile: all_profile_data.extend(profile)
                if zeroed: all_zeroed_data.extend(zeroed)

                combined_row = self._combine_data_rows(current_date, device_name, sublot_name, imp_row, sqmm_row, thick_metrics)
                output_data.append(combined_row)
                
            if export_profile and all_profile_data:
                self.save_thickness_profile(output_folder, sublot_name, all_profile_data, all_zeroed_data)
                
            print(f"=== 结束处理 Subfolder: {sublot_name} ===\n")

        except Exception as e:
            print(f"!!! Error processing subfolder {subfolder_path}: {e}")
            import traceback
            print(traceback.format_exc())

    # 注意：函数签名多了一个 timestamp_suffix 参数
    # 函数签名增加了 expected_wafers
    def _find_thickness_file(self, device_name: str, subfolder_path: str, fpms007_as_dp: bool, thick_filename_local: Optional[str], acq_time_for_search: Optional[datetime], timestamp_suffix: str = None, expected_wafers: int = 0, search_stats: dict = None, sublot_trace_logs: list = None, allow_wide_search: bool = True) -> Optional[str]:
        
        def log_thk(msg):
            print(msg)
            if sublot_trace_logs is not None:
                sublot_trace_logs.append(msg)

        sublot_name = os.path.basename(subfolder_path)
        log_thk(f"\n[DEBUG-THK] >>> 寻找厚度文件, 设备: {device_name}")

        best_partial_file = None
        best_wafer_count = -1
        best_file_size = -1

        def evaluate_and_check_perfect(filepath: str) -> bool:
            nonlocal best_partial_file, best_wafer_count, best_file_size
            try:
                size = os.path.getsize(filepath)
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                
                header_idx = next((i for i, line in enumerate(lines) if "Wafer ID" in line), -1)
                if header_idx == -1: return False
                
                data_lines = [l.strip() for l in lines[header_idx+1:] if l.strip()]
                wafer_count = len(data_lines)
                
                is_perfect = True
                if wafer_count < expected_wafers:
                    log_thk(f"[DEBUG-THK] 质检瑕疵: {os.path.basename(filepath)} 晶圆数不足 ({wafer_count}/{expected_wafers})")
                    is_perfect = False
                else:
                    last_row = data_lines[-1].split(',')
                    col_idx = Config.ThicknessFile.COL_754_IDX
                    if len(last_row) <= col_idx or not last_row[col_idx].strip():
                        log_thk(f"[DEBUG-THK] 质检瑕疵: {os.path.basename(filepath)} 最后一行未写完")
                        is_perfect = False
                
                if not is_perfect:
                    if wafer_count > best_wafer_count or (wafer_count == best_wafer_count and size > best_file_size):
                        best_partial_file = filepath
                        best_wafer_count = wafer_count
                        best_file_size = size
                        log_thk(f"[DEBUG-THK] -> 更新最佳备胎: {os.path.basename(filepath)} (晶圆: {wafer_count})")
                        
                return is_perfect
            except Exception as e:
                log_thk(f"[DEBUG-THK] 验货发生异常: {e}")
                return False

        exact_suffix = timestamp_suffix
        
        # ⭐ 改造时间生成器：支持指定范围，方便分阶梯调用
        def get_offset_suffixes(start_sec, end_sec):
            offsets = []
            if not exact_suffix: return offsets
            try:
                from datetime import datetime, timedelta
                if len(exact_suffix) == 12:
                    base_dt = datetime.strptime(exact_suffix, "%y%m%d%H%M%S")
                    for offset in range(start_sec, end_sec + 1):
                        offsets.append((base_dt - timedelta(seconds=offset)).strftime("%y%m%d%H%M%S"))
                        offsets.append((base_dt + timedelta(seconds=offset)).strftime("%y%m%d%H%M%S"))
                elif len(exact_suffix) == 14:
                    base_dt = datetime.strptime(exact_suffix, "%Y%m%d%H%M%S")
                    for offset in range(start_sec, end_sec + 1):
                        offsets.append((base_dt - timedelta(seconds=offset)).strftime("%Y%m%d%H%M%S"))
                        offsets.append((base_dt + timedelta(seconds=offset)).strftime("%Y%m%d%H%M%S"))
            except ValueError:
                pass
            return offsets

        # =========================================================
        # 阶段 1：本地目录极速查找
        # =========================================================
        local_dirs = [subfolder_path]
        if device_name == "DPGE101":
            local_dirs.append(os.path.dirname(subfolder_path))
            
        local_dirs_ordered = list(dict.fromkeys(local_dirs))
        
        if exact_suffix:
            for d in local_dirs_ordered:
                if not os.path.isdir(d): continue
                guess_local_exact = os.path.join(d, f"Thickness_Sector_Height_Profile_Sectors_1_Inner_Radius_150mm_(HiRes)-{exact_suffix}.csv")
                if os.path.exists(guess_local_exact):
                    log_thk(f"[DEBUG-THK] 发现本地精确匹配候选: {os.path.basename(guess_local_exact)}，开始质检...")
                    if evaluate_and_check_perfect(guess_local_exact):
                        log_thk("[DEBUG-THK] 质检100分！使用本地完美文件。")
                        if search_stats is not None: search_stats['local'].append(sublot_name)
                        return guess_local_exact

        for d in local_dirs_ordered:
            if not os.path.isdir(d): continue
            try:
                for f in os.listdir(d):
                    if f.lower().startswith("thickness") and "150mm" in f.lower() and f.lower().endswith(".csv"):
                        p = os.path.join(d, f)
                        log_thk(f"[DEBUG-THK] 发现本地容差/备胎候选: {f}，开始质检...")
                        if evaluate_and_check_perfect(p):
                            log_thk("[DEBUG-THK] 容差质检100分！使用本地文件。")
                            if search_stats is not None: search_stats['local'].append(sublot_name)
                            return p
            except OSError:
                continue

        # 统一提取真实机台时间戳
        local_thk_ts = None
        if best_partial_file:
            m = re.search(r'[-_](\d{12,14})\.csv$', os.path.basename(best_partial_file), re.IGNORECASE)
            if m:
                local_thk_ts = m.group(1)

        log_thk("[DEBUG-THK] 本地无满分文件，准备进入中央库寻找...")

        central_dirs = []
        templates = [
            Config.ERO_ERROR_PATH_TEMPLATE,
            Config.ERO_POST_PATH_TEMPLATE,
            Config.ERO_PRE_PATH_TEMPLATE,
            Config.THK_PROFILE_PATH_TEMPLATE,
        ]
        for t in templates:
            if device_name == "FPMS007" and fpms007_as_dp and t in [Config.ERO_PRE_PATH_TEMPLATE, Config.ERO_POST_PATH_TEMPLATE]:
                continue
            p = t.format(device=device_name)
            central_dirs.append(p)
            if "Success" in p:
                central_dirs.append(p.replace("\\Success", "").replace("/Success", ""))
                
        central_dirs_ordered = list(dict.fromkeys(central_dirs))

        # =========================================================
        # 阶段 2：中央库【第 1 发狙击：IMP 原始时间】
        # =========================================================
        if exact_suffix:
            exact_target_name = f"Thickness_Sector_Height_Profile_Sectors_1_Inner_Radius_150mm_(HiRes)-{exact_suffix}.csv"
            for d in central_dirs_ordered:
                if not os.path.isdir(d): continue
                guess_path = os.path.join(d, exact_target_name)
                
                if os.path.exists(guess_path):
                    log_thk(f"[DEBUG-THK] IMP单发盲狙命中目标！开始质检: {guess_path}")
                    if evaluate_and_check_perfect(guess_path):
                        log_thk("[DEBUG-THK] 质检100分！瞬间返回。")
                        if search_stats is not None: search_stats['imp_single'].append(sublot_name)
                        return guess_path

        # =========================================================
        # 阶段 3：中央库【第 2 发狙击：提取到的真实 THK 时间】
        # =========================================================
        if local_thk_ts and local_thk_ts != exact_suffix:
            log_thk(f"[DEBUG-THK] 启用机台真实时间({local_thk_ts})进行 THK单发盲狙...")
            exact_target_name_thk = f"Thickness_Sector_Height_Profile_Sectors_1_Inner_Radius_150mm_(HiRes)-{local_thk_ts}.csv"
            for d in central_dirs_ordered:
                if not os.path.isdir(d): continue
                guess_path = os.path.join(d, exact_target_name_thk)
                
                if os.path.exists(guess_path):
                    log_thk(f"[DEBUG-THK] THK单发盲狙命中目标！开始质检: {guess_path}")
                    if evaluate_and_check_perfect(guess_path):
                        log_thk("[DEBUG-THK] 质检100分！瞬间返回。")
                        if search_stats is not None: search_stats['thk_single'].append(sublot_name)
                        return guess_path

        # =========================================================
        # 阶段 4：中央库【阶梯式火力扫射】
        # =========================================================
        
        if allow_wide_search:
            # 4.1 第一阶梯：近战点射 (±1秒)
            offset_tier1 = get_offset_suffixes(1, 1) # 只生成 1秒的前后偏移，共2个时间戳
            if offset_tier1:
                log_thk("[DEBUG-THK] 狙击落空，启动【近距点射盲狙】(±1秒)...")
                for d in central_dirs_ordered:
                    if not os.path.isdir(d): continue
                    for s in offset_tier1:
                        guess_name = f"Thickness_Sector_Height_Profile_Sectors_1_Inner_Radius_150mm_(HiRes)-{s}.csv"
                        guess_path = os.path.join(d, guess_name)
                        
                        if os.path.exists(guess_path):
                            log_thk(f"[DEBUG-THK] 近距盲狙命中(后缀:{s})！开始质检: {guess_path}")
                            if evaluate_and_check_perfect(guess_path):
                                log_thk("[DEBUG-THK] 质检100分！瞬间返回。")
                                if search_stats is not None: search_stats['multi_tier1'].append((sublot_name, list(sublot_trace_logs)))
                                return guess_path
                            
            # 4.2 第二阶梯：远距扫射 (±2 到 ±10秒)
            offset_tier2 = get_offset_suffixes(2, 10) # 扩大到 2~10秒，共18个时间戳
            if offset_tier2:
                log_thk("[DEBUG-THK] 近距点射落空，启动【远距火力扫射】(±2~10秒)...")
                for d in central_dirs_ordered:
                    if not os.path.isdir(d): continue
                    for s in offset_tier2:
                        guess_name = f"Thickness_Sector_Height_Profile_Sectors_1_Inner_Radius_150mm_(HiRes)-{s}.csv"
                        guess_path = os.path.join(d, guess_name)
                        
                        if os.path.exists(guess_path):
                            log_thk(f"[DEBUG-THK] 远距盲狙命中(后缀:{s})！开始质检: {guess_path}")
                            if evaluate_and_check_perfect(guess_path):
                                log_thk("[DEBUG-THK] 质检100分！瞬间返回。")
                                if search_stats is not None: search_stats['multi_tier2'].append((sublot_name, list(sublot_trace_logs)))
                                return guess_path

        # =========================================================
        # 阶段 5：降级保底
        # =========================================================
        if best_partial_file:
            log_thk(f"[DEBUG-THK] <<< 未能找到满分文件，启用降级方案！")
            log_thk(f"[DEBUG-THK] <<< 返回收集到的【最佳残缺文件】: {best_partial_file} (包含晶圆数: {best_wafer_count})")
            if search_stats is not None: search_stats['fallback'].append((sublot_name, list(sublot_trace_logs)))
            return best_partial_file

        log_thk("[DEBUG-THK] <<< 彻底失败：按顺序盲狙完所有中央库，且无任何残缺件可用，返回 None。")
        if search_stats is not None: search_stats['failed'].append((sublot_name, list(sublot_trace_logs)))
        return None
    def _format_acquisition_time(self, time_str: Optional[str]) -> str:
        """
        Tries to parse a date string and reformats it to 'YYYY/M/D H:M:S'.
        """
        if not time_str or not time_str.strip():
            return ""

        time_str = time_str.strip()
        possible_formats = [
            '%m/%d/%y %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M:%S',
            '%Y/%m/%d %H:%M',
        ]

        for fmt in possible_formats:
            try:
                dt_obj = datetime.strptime(time_str, fmt)
                # --- 修改：强制转换为 2026/2/1 2:40:29 格式 (无前导零) ---
                return f"{dt_obj.year}/{dt_obj.month}/{dt_obj.day} {dt_obj.hour}:{dt_obj.minute}:{dt_obj.second}"
            except ValueError:
                continue
        
        return time_str

    def _combine_data_rows(self, date, device, sublot, imp_row, sqmm_row, thick_metrics) -> list:
        """Combines data from all sources into a single row list for the final CSV."""
        
        # --- 安全补丁：确保 tm 列表长度足够 ---
        # 即使厚度文件读取失败，也要补齐 None，保证 CSV 列数对齐，防止错位
        tm = list(thick_metrics) if thick_metrics else [None] * 9
        while len(tm) < 9:
            tm.append(None)
        
        acquisition_time_str = imp_row.get('Acquisition Date/Time')
        formatted_time = self._format_acquisition_time(acquisition_time_str)

        sfqrp99_value = None
        if device in ('FPMS004', 'FPMS007'):
            sfqrp99_value = imp_row.get('SFQR Value @ 98 % (um)')
        else:
            sfqrp99_value = imp_row.get('SFQR Value @ 99 % (um)')

        return [
            date.strftime('%Y%m%d'), device, sublot,
            imp_row.get('Wafer ID'), imp_row.get('Source Slot'), formatted_time,
            
            # --- 修正: 顺序必须与 Headers 严格一致 ---
            imp_row.get('Mean Thickness (um)'),    # 1. Mean
            imp_row.get('Center Thickness (um)'), # 2. Center (Added)
            
            imp_row.get('GBIR (um)'), imp_row.get('GFLR (um)'),
            imp_row.get('GMLYMCD (Bow-BF) (um)'), imp_row.get('GMLYMER (Warp-BF) (um)'),
            imp_row.get('SFQR Maximum (um)'),
            sfqrp99_value, # Use the conditionally obtained value
            imp_row.get('ESFQR Maximum (um)'),
            imp_row.get('Front Sector ZDD  Sectors: 72 @ 148 mm Mean (nm / mm^2)'),
            sqmm_row.get('Front THA (2 mm Square PV) @ 0.05 % (nm)'),
            sqmm_row.get('Front THA (10 mm Square PV) @ 0.05 % (nm)'),
            sqmm_row.get('Front THA (2 mm Square PV) @ 0 % (nm)'),
            sqmm_row.get('Front THA (10 mm Square PV) @ 0 % (nm)'),
            
            # Thickness Metrics (从 tm[0] 到 tm[8])
            tm[0], # ERO147
            tm[1], # ERO148
            tm[2], # ERO149
            tm[3], # MaxR
            tm[4], # MaxE
            tm[5], # Convexity
            tm[6], # Edge
            tm[7], # Center_Slope
            tm[8]  # Mid_Slope (这是最后一项)
        ]

    def _read_csv_file(self, folder: str, prefix: str) -> Optional[Tuple[List[Dict], List[str]]]:
        """Robustly reads a CSV file into a list of dictionaries."""
        try:
            filename = next((f for f in os.listdir(folder) if f.startswith(prefix) and f.endswith(".csv")), None)
            if not filename: return None

            with open(os.path.join(folder, filename), 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            header_idx = next((i for i, line in enumerate(lines) if "Wafer ID" in line and "Acquisition Date/Time" in line), None)
            if header_idx is None: return None
            
            lines[header_idx] = lines[header_idx].lstrip('\ufeff')

            reader = csv.reader(lines[header_idx:])
            header = [h.strip() for h in next(reader)]
            
            data = []
            for row in reader:
                if row:
                    row_dict = dict(zip(header, row))
                    data.append(row_dict)

            return data, header
        except Exception as e:
            print(f"Failed to read file with prefix {prefix} in {folder}: {e}")
            return None

    def _write_output_csv(self, output_folder: str, data: list, headers: list):
        """Writes the collected data to the final CSV file."""
        output_file = os.path.join(output_folder, f"{os.path.basename(output_folder)}.csv")
        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(data)

    def _find_subfolders(self, base_path: str, prefixes: List[str]) -> List[str]:
        """Finds subfolders matching given prefixes, or all subfolders if no prefixes are provided."""
        if not prefixes:
            try:
                return [os.path.join(base_path, d) for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
            except OSError:
                return []
        else:
            found_folders = []
            for prefix in prefixes:
                try:
                    found_folders.extend(glob.glob(os.path.join(base_path, f"{prefix}*")))
                except OSError:
                    continue
            return [f for f in found_folders if os.path.isdir(f)]

    def on_topo_cancel(self):
        """Cancels the TOPO data processing."""
        self.app.stop_event.set()
        messagebox.showinfo("信息", "已发送取消请求。进程将很快停止。")

    def _set_controls_state(self, enable: bool):
        """Enables or disables UI controls during processing."""
        state = tk.NORMAL if enable else tk.DISABLED
        readonly_state = "readonly" if enable else tk.DISABLED
        
        self.topo_read_button.config(state=state)
        self.topo_cancel_button.config(state=tk.DISABLED if enable else tk.NORMAL)
        
        self.topo_start_date.config(state=state)
        self.topo_end_date.config(state=state)
        self.device_listbox.config(state=state)
        self.subfolder_prefix_entry.config(state=state)
        self.file_prefix_dropdown.config(state=readonly_state)
        if enable:
            self.topo_progress_label.config(text="准备就绪")

    def save_thickness_profile(self, output_folder, sublot_id, profile_data, zeroed_profile_data):
        """Saves thickness profile data to CSV and generates a chart."""
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(12, 6))
        try:
            self._append_to_profile_csv(output_folder, "Thickness_Profile", profile_data)
            self._append_to_profile_csv(output_folder, "Thickness_0Profile", zeroed_profile_data)

            num_plots = len(zeroed_profile_data)
            colors = plt.cm.jet(np.linspace(0, 1, num_plots)) if num_plots > 1 else ['b']
            x_values = [x * 0.2 for x in range(len(zeroed_profile_data[0]) - 1)]

            for idx, row in enumerate(zeroed_profile_data):
                wafer_id, y_values = row[0], row[1:]
                valid_indices = [i for i, y in enumerate(y_values) if not np.isnan(y)]
                if valid_indices:
                    ax.plot([x_values[i] for i in valid_indices], [y_values[i] for i in valid_indices],
                            label=wafer_id, color=colors[idx % len(colors)])

            ax.set_xlabel('Position (mm)')
            ax.set_ylabel('Thickness (um)')
            ax.set_title(f'Zeroed Thickness Profile - {sublot_id}')
            ax.grid(True)
            if num_plots > 1:
                ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='x-small', ncol=2 if num_plots > 10 else 1)
            
            plt.tight_layout()
            folder_basename = os.path.basename(output_folder)
            plot_file = os.path.join(output_folder, "Fig", f"{folder_basename}_{sublot_id}.png")
            fig.savefig(plot_file, dpi=200, bbox_inches='tight')

        except Exception as e:
            print(f"Error saving thickness profile for {sublot_id}: {e}")
        finally:
            plt.close(fig)
            plt.close('all')
            gc.collect()

    def _append_to_profile_csv(self, output_folder, file_prefix, data_rows):
        """Appends data to a profile CSV, writing headers if the file is new."""
        if not data_rows or not data_rows[0]: return
        profile_file = os.path.join(output_folder, f"{file_prefix}_{os.path.basename(output_folder)}.csv")
        file_exists = os.path.exists(profile_file)
        with open(profile_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                headers = ['Wafer ID'] + [str(x * 0.2) for x in range(len(data_rows[0]) - 1)]
                writer.writerow(headers)
            writer.writerows(data_rows)