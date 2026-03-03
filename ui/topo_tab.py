import os
import tkinter as tk
from tkinter import ttk, messagebox, Checkbutton
from tkcalendar import DateEntry
import csv
import glob
import re
import gc
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
import numpy as np

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from config import Config
from utils import FileProcessor

class TopoDataFunction:
    """Implements the TOPO DATA feature."""
    def __init__(self, app):
        self.app = app
        self.frame = None
        self.fpms007_dp_var = tk.BooleanVar(value=True)
    
    def show(self):
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

        device_list = [f"FPMS{num:03d}" for num in range(1, 13)]
        device_list.append("DPGE101")
        
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
        if device_name == "DPGE101":
            return os.path.join(Config.DPGE101_BASE_PATH, current_date.strftime('%Y%m%d'))

        if current_date >= Config.PATH_TRANSITION_DATE:
            base = Config.NEW_BASE_PATH
            if isinstance(base, list):
                base = base[0] 
            return os.path.join(str(base), f"01_{device_name}", "01_Production", current_date.strftime('%Y%m%d'))
        else:
            return os.path.join(Config.OLD_BASE_PATH, f"02_{device_name}", "01_Production", current_date.strftime('%Y%m%d'))

    def start_topo_processing_thread(self):
        self.app.start_thread(self._process_topo_data_from_ui, self._set_controls_state)

    def _process_topo_data_from_ui(self):
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

    def execute_topo_processing(self, start_date, end_date, devices, lot_prefixes, file_prefix, export_profile, fpms007_as_dp, output_suffix: str = "", custom_prefix: str = "") -> Tuple[Optional[str], Optional[List], Optional[List]]:
        timestamp = datetime.now().strftime('%H%M%S')
        
        if custom_prefix:
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
            "SFQR Max (um)", "SFQRP99(um)", "ESFQR Max (um)", "ZDD (nm/mm^2)", "THA 2mm (nm)", "THA 10mm (nm)","THA 2mm 0%(nm)", "THA 10mm 0%(nm)",
            "ERO147", "ERO148", "ERO149", "MaxR", "MaxE",
            "Convexity", "Edge", "Center_Slope", "Mid_Slope"
        ]
        
        for i, current_date in enumerate(date_range):
            if self.app.stop_event.is_set(): break
            
            progress = (i + 1) / total_days * 100
            self.app.update_progress(f"正在处理日期 {current_date.strftime('%Y%m%d')} (批次: {output_suffix})...", progress, 'topo')
            
            for device_name in devices:
                if self.app.stop_event.is_set(): break
                self._process_device_for_date(current_date, device_name, lot_prefixes, file_prefix, export_profile, fpms007_as_dp, output_folder, output_data)

        if not self.app.stop_event.is_set() and output_data:
            self._write_output_csv(output_folder, output_data, headers)
            self.app.update_progress(f"{output_suffix} 处理完成！结果已保存。", 100, 'topo')
            return os.path.abspath(output_folder), output_data, headers

        return None, None, None

    def _gather_ui_inputs(self) -> Optional[Dict[str, Any]]:
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

    def _process_device_for_date(self, current_date, device_name, lot_prefixes, file_prefix, export_profile, fpms007_as_dp, output_folder, output_data):
        base_folder_path = self._get_base_folder_path(current_date, device_name)
        if not os.path.exists(base_folder_path): return
        
        subfolders = self._find_subfolders(base_folder_path, lot_prefixes)
        for subfolder in subfolders:
            if self.app.stop_event.is_set(): break
            self._process_subfolder(subfolder, current_date, device_name, file_prefix, export_profile, fpms007_as_dp, output_folder, output_data)

    def _process_subfolder(self, subfolder_path, current_date, device_name, file_prefix, export_profile, fpms007_as_dp, output_folder, output_data):
        try:
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
            
            thick_filename_local = next((f for f in os.listdir(subfolder_path) if f.startswith(Config.THICKNESS_PREFIX)), None)
            
            thick_file_path_to_use = self._find_thickness_file(
                device_name=device_name,
                subfolder_path=subfolder_path,
                fpms007_as_dp=fpms007_as_dp,
                thick_filename_local=thick_filename_local,
                acq_time_for_search=acq_time_for_search
            )

            all_profile_data, all_zeroed_data = [], []
            
            for imp_row in imp_data:
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

                combined_row = self._combine_data_rows(current_date, device_name, os.path.basename(subfolder_path), imp_row, sqmm_row, thick_metrics)
                output_data.append(combined_row)
                
            if export_profile and all_profile_data:
                self.save_thickness_profile(output_folder, os.path.basename(subfolder_path), all_profile_data, all_zeroed_data)
        except Exception as e:
            print(f"Error processing subfolder {subfolder_path}: {e}")

    def _find_thickness_file(self, device_name: str, subfolder_path: str, fpms007_as_dp: bool, thick_filename_local: Optional[str], acq_time_for_search: Optional[datetime]) -> Optional[str]:
        debug_enabled = Config.DEBUG_THICKNESS_SEARCH and device_name == "DPGE101"
        if debug_enabled:
            print(f"[DPGE101][THK] subfolder_path={subfolder_path}")
            print(f"[DPGE101][THK] thick_filename_local={thick_filename_local}")
            print(f"[DPGE101][THK] acq_time_for_search={acq_time_for_search}")

        def _pick_by_time(files: List[str]) -> Optional[str]:
            if not files:
                return None
            if not acq_time_for_search:
                return max(files, key=lambda f: os.path.getmtime(f))
            target_ts = acq_time_for_search.timestamp()
            return min(files, key=lambda f: abs(os.path.getmtime(f) - target_ts))

        def _collect_thickness_candidates(
            search_dir: str,
            include_children: bool = False,
            prefixes: Optional[List[str]] = None
        ) -> List[str]:
            if not os.path.isdir(search_dir):
                return []
            candidates: List[str] = []
            normalized_prefixes = [re.sub(r"[^a-z0-9]", "", p.lower()) for p in (prefixes or [Config.THICKNESS_PREFIX])]
            try:
                for filename in os.listdir(search_dir):
                    file_lower = filename.lower()
                    normalized_name = re.sub(r"[^a-z0-9]", "", file_lower)
                    if file_lower.endswith(".csv") and any(normalized_name.startswith(p) for p in normalized_prefixes):
                        candidates.append(os.path.join(search_dir, filename))
            except OSError:
                return candidates

            if include_children:
                try:
                    subdirs = [
                        os.path.join(search_dir, child)
                        for child in os.listdir(search_dir)
                        if os.path.isdir(os.path.join(search_dir, child))
                    ]
                except OSError:
                    subdirs = []
                for subdir in subdirs:
                    try:
                        for filename in os.listdir(subdir):
                            file_lower = filename.lower()
                            normalized_name = re.sub(r"[^a-z0-9]", "", file_lower)
                            if file_lower.endswith(".csv") and any(normalized_name.startswith(p) for p in normalized_prefixes):
                                candidates.append(os.path.join(subdir, filename))
                    except OSError:
                        continue
            if debug_enabled:
                print(f"[DPGE101][THK] candidates in {search_dir} (children={include_children}) -> {len(candidates)}")
            return candidates

        if thick_filename_local:
            search_paths_by_name = [
                Config.ERO_ERROR_PATH_TEMPLATE,
                Config.ERO_POST_PATH_TEMPLATE,
                Config.THK_PROFILE_PATH_TEMPLATE,
            ]
            if device_name == "FPMS004":
                search_paths_by_name.insert(1, Config.ERO_PRE_PATH_TEMPLATE)

            for path_template in search_paths_by_name:
                if device_name == "FPMS007" and fpms007_as_dp and path_template in [Config.ERO_PRE_PATH_TEMPLATE, Config.ERO_POST_PATH_TEMPLATE]:
                    continue

                potential_path = os.path.join(path_template.format(device=device_name), thick_filename_local)
                if os.path.exists(potential_path):
                    if debug_enabled:
                        print(f"[DPGE101][THK] match by name: {potential_path}")
                    return potential_path

            fallback_path = os.path.join(subfolder_path, thick_filename_local)
            if os.path.exists(fallback_path):
                if debug_enabled:
                    print(f"[DPGE101][THK] match local fallback: {fallback_path}")
                return fallback_path
            if device_name == "DPGE101":
                local_candidates = []
                date_dir = os.path.dirname(subfolder_path)
                dpge_prefixes = [
                    Config.THICKNESS_PREFIX.lower(),
                    "thickness",
                    Config.THK_SECTOR_PREFIX.lower(),
                    "thickness sector height profile sectors 1 inner radius 150mm"
                ]
                local_candidates.extend(_collect_thickness_candidates(subfolder_path, prefixes=dpge_prefixes))
                local_candidates.extend(_collect_thickness_candidates(date_dir, include_children=True, prefixes=dpge_prefixes))
                selected = _pick_by_time(local_candidates)
                if selected:
                    if debug_enabled:
                        print(f"[DPGE101][THK] match local candidates: {selected}")
                    return selected
        else:
            if device_name == "DPGE101":
                local_candidates = []
                date_dir = os.path.dirname(subfolder_path)
                dpge_prefixes = [
                    Config.THICKNESS_PREFIX.lower(),
                    "thickness",
                    Config.THK_SECTOR_PREFIX.lower(),
                    "thickness sector height profile sectors 1 inner radius 150mm"
                ]
                local_candidates.extend(_collect_thickness_candidates(subfolder_path, prefixes=dpge_prefixes))
                local_candidates.extend(_collect_thickness_candidates(date_dir, include_children=True, prefixes=dpge_prefixes))
                selected = _pick_by_time(local_candidates)
                if selected:
                    if debug_enabled:
                        print(f"[DPGE101][THK] match local candidates (no name): {selected}")
                    return selected

        if acq_time_for_search:
            search_paths_by_time = [
                Config.ERO_PRE_PATH_TEMPLATE,
                Config.ERO_POST_PATH_TEMPLATE,
                Config.ERO_ERROR_PATH_TEMPLATE,
                Config.THK_PROFILE_PATH_TEMPLATE,
            ]
            extra_search_dirs = []
            if device_name == "DPGE101":
                extra_search_dirs.extend([subfolder_path, os.path.dirname(subfolder_path)])
            
            time_window = timedelta(minutes=5)
            start_time = acq_time_for_search - time_window
            end_time = acq_time_for_search + time_window

            for path_template in search_paths_by_time:
                search_dir = path_template.format(device=device_name)
                if not os.path.isdir(search_dir):
                    continue

                try:
                    for filename in os.listdir(search_dir):
                        if filename.startswith(Config.THICKNESS_PREFIX) and filename.endswith(".csv"):
                            file_path = os.path.join(search_dir, filename)
                            try:
                                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                                if start_time <= mod_time <= end_time:
                                    if debug_enabled:
                                        print(f"[DPGE101][THK] match by time: {file_path}")
                                    return file_path
                            except OSError:
                                continue
                except OSError:
                    continue

            for search_dir in extra_search_dirs:
                if not os.path.isdir(search_dir):
                    continue
                try:
                    for filename in os.listdir(search_dir):
                        if filename.startswith(Config.THICKNESS_PREFIX) and filename.endswith(".csv"):
                            file_path = os.path.join(search_dir, filename)
                            try:
                                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                                if start_time <= mod_time <= end_time:
                                    if debug_enabled:
                                        print(f"[DPGE101][THK] match by time (local): {file_path}")
                                    return file_path
                            except OSError:
                                continue
                except OSError:
                    continue
        if debug_enabled:
            print("[DPGE101][THK] no thickness file matched.")

        return None

    def _format_acquisition_time(self, time_str: Optional[str]) -> str:
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
                return f"{dt_obj.year}/{dt_obj.month}/{dt_obj.day} {dt_obj.hour}:{dt_obj.minute}:{dt_obj.second}"
            except ValueError:
                continue
        
        return time_str

    def _combine_data_rows(self, date, device, sublot, imp_row, sqmm_row, thick_metrics) -> list:
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
            imp_row.get('Mean Thickness (um)'),    
            imp_row.get('Center Thickness (um)'), 
            imp_row.get('GBIR (um)'), imp_row.get('GFLR (um)'),
            imp_row.get('GMLYMCD (Bow-BF) (um)'), imp_row.get('GMLYMER (Warp-BF) (um)'),
            imp_row.get('SFQR Maximum (um)'),
            sfqrp99_value, 
            imp_row.get('ESFQR Maximum (um)'),
            imp_row.get('Front Sector ZDD  Sectors: 72 @ 148 mm Mean (nm / mm^2)'),
            sqmm_row.get('Front THA (2 mm Square PV) @ 0.05 % (nm)'),
            sqmm_row.get('Front THA (10 mm Square PV) @ 0.05 % (nm)'),
            sqmm_row.get('Front THA (2 mm Square PV) @ 0 % (nm)'),
            sqmm_row.get('Front THA (10 mm Square PV) @ 0 % (nm)'),
            tm[0], tm[1], tm[2], tm[3], tm[4], tm[5], tm[6], tm[7], tm[8]  
        ]

    def _read_csv_file(self, folder: str, prefix: str) -> Optional[Tuple[List[Dict], List[str]]]:
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
        output_file = os.path.join(output_folder, f"{os.path.basename(output_folder)}.csv")
        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(data)

    def _find_subfolders(self, base_path: str, prefixes: List[str]) -> List[str]:
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
        self.app.stop_event.set()
        messagebox.showinfo("信息", "已发送取消请求。进程将很快停止。")

    def _set_controls_state(self, enable: bool):
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
        if not data_rows or not data_rows[0]: return
        profile_file = os.path.join(output_folder, f"{file_prefix}_{os.path.basename(output_folder)}.csv")
        file_exists = os.path.exists(profile_file)
        with open(profile_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                headers = ['Wafer ID'] + [str(x * 0.2) for x in range(len(data_rows[0]) - 1)]
                writer.writerow(headers)
            writer.writerows(data_rows)