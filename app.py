# v8
# 1.DP_FP处理数据，排序升序处理优化。
# 2.新增NT 0%
# 3.sublot自动化流程，数量超过2，则sublot1-sublot2.etc~命名
# 4.优化查询厚度文件计算ERO
# 5.removal和追溯历史文件自动输出
import os
import sys
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Checkbutton, scrolledtext
from tkcalendar import DateEntry
import csv
import jaydebeapi
import jpype # 必须引入 jpype，jaydebeapi 底层依赖它
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd  
import threading
import glob
import shutil
from datetime import datetime, timedelta
import gc
from typing import List, Tuple, Optional, Dict, Any
import re
import seaborn as sns
import warnings
# def print(msg):
#     """将日志同时打印到控制台并追加到本地文件，带毫秒级时间戳"""
#     t = datetime.now().strftime('%H:%M:%S.%f')[:-3]
#     full_msg = f"[{t}] {msg}"
#     print(full_msg)
#     try:
#         with open("debug_topo_log.txt", "a", encoding="utf-8") as f:
#             f.write(full_msg + "\n")
#     except Exception:
#         pass

# --- Global Plotting Style for Data Report ---
# Setting font to Times New Roman for professional reporting.
# Updated Chinese support: Removing 'bold' from charts is key to fixing the glyph issue.
# We set the style AFTER importing seaborn to ensure overrides work.
sns.set_theme(style="whitegrid", palette="Set2")

# Explicitly update matplotlib params AFTER seaborn theme
# Fallback chain: Times New Roman -> 楷体 (Chinese) -> KaiTi -> Microsoft YaHei
plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = ['Times New Roman', '楷体', 'KaiTi', 'Microsoft YaHei', 'SimHei']
plt.rcParams['font.sans-serif'] = ['Times New Roman', '楷体', 'KaiTi', 'Microsoft YaHei', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False  # Fix display of minus sign

# Suppress specific Matplotlib warnings
warnings.filterwarnings("ignore", category=UserWarning, message="Starting a Matplotlib GUI outside of the main thread")
warnings.filterwarnings("ignore", message="This figure includes Axes that are not compatible with tight_layout")

# Stage Options
STAGE_OPTIONS = [
    "DP", "FP",
    "PRE_2000", "POST_2000",
    "POST_DP", "POST_POLY",
    "POST_LTO", "POST_POLY_LTO",
    "PRE_FP", "POST_FP",
    "PRE_EPI", "POST_EPI"
]

class Config:
    PATH_TRANSITION_DATE = datetime.strptime("20250714", "%Y%m%d").date()

    NEW_BASE_PATH = [r"\\FAKE_IP_WHDLM7YB\Analytical_FPMS2",r"\\FAKE_IP_WHDLM7YB\Analytical_Machine2\05_DPGE\02_DPGE101\01_Production"]
    OLD_BASE_PATH = r"\\FAKE_IP_WHDLM7YB\Analytical_Machine2\07_FPMS"

    DPGE101_BASE_PATH =r"\\FAKE_IP_WHDLM7YB\Analytical_Machine2\05_DPGE\02_DPGE101\01_Production"
    DEBUG_THICKNESS_SEARCH = False
    
    ERO_ERROR_PATH_TEMPLATE = r"\\FAKE_IP_WHDLM7YB\Analytical_Machine2\07_FPMS\00_ERO_ERROR\{device}"
    ERO_POST_PATH_TEMPLATE = r"\\FAKE_IP_WHDLM7YB\Analytical_Machine2\07_FPMS\00_ERO_POST\{device}"
    ERO_PRE_PATH_TEMPLATE = r"\\FAKE_IP_WHDLM7YB\Analytical_Machine2\07_FPMS\00_ERO_PRE\{device}\Success"
    THK_PROFILE_PATH_TEMPLATE = r"\\FAKE_IP_WHDLM7YB\Analytical_Machine2\07_FPMS\00_THK_Profile\{device}"
    
    IMP_PREFIXES = ["IMP_W26D08_D35S72_EE1", "IMP_W26D08_D35S72_EE2"]
    THICKNESS_PREFIX = "Thickness_"
    SQMM_PREFIXES = ["SQMM-", "SQMM_"]

    THK_SECTOR_PREFIX = "Thickness_Sector_Height_Profile_Sectors_1_Inner_Radius_150mm_(HiRes)-"

    class ThicknessFile:
        WAFER_ID_COL_NAME = "Wafer ID"
        # Column indices (0-based)
        COL_9_IDX = 8
        COL_359_IDX = 358
        COL_384_IDX = 383
        COL_609_IDX = 608
        COL_709_IDX = 708
        COL_734_IDX = 733
        COL_744_IDX = 743
        COL_749_IDX = 748
        COL_754_IDX = 753
        COL_756_IDX = 755
        MIN_REQUIRED_COLS = 756
        PROFILE_START_COL = 8
        PROFILE_END_COL = 757


import sys
import os
import time
import shutil
import traceback
import threading
from tkinter import messagebox
import jpype
import jaydebeapi

class DatabaseManager:
    """Manages database connections (Ultra-High-Speed Singleton + Local Sync Edition)."""

    _cached_java_home = None
    _jvm_started = False
    _cached_conn = None  # 全局长连接缓存
    _init_lock = threading.Lock() # 新增：防多线程抢跑锁

    @staticmethod
    def get_resource_path(relative_path):
        if getattr(sys, 'frozen', False):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(base_path, relative_path)

    @classmethod
    def ping_connection(cls):
        """测试当前缓存的连接是否仍然有效"""
        if not cls._cached_conn:
            return False
        try:
            with cls._cached_conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1") # DB2 保活查询
            return True
        except Exception:
            cls._cached_conn = None # 连接失效，清空缓存
            return False

    @classmethod
    def _get_optimal_jdk_path(cls):
        """核心提速逻辑：尝试获取本地 JDK，如果只有网络盘，则同步到本地"""
        # 设定本地缓存目录: C:\Users\<用户名>\.topo_app_env\jdk-21
        local_env_dir = os.path.join(os.path.expanduser("~"), ".topo_app_env")
        local_jdk_path = os.path.join(local_env_dir, "jdk-21_windows-x64_bin")
        success_flag = os.path.join(local_jdk_path, ".copy_success") # 新增：拷贝完成安全标志

        # 1. 极速通道：只有当成功标志存在时，才认为本地 JDK 是完整健康的
        if os.path.exists(success_flag):
            return local_jdk_path

        # 2. 寻找可用的网络盘 JDK
        network_paths = [
            r"\\FAKE_IP_YDJLHUU5\中段工艺\98_Common\吴广\jdk-21_windows-x64_bin",
            r"\\FAKE_IP_YDJLHUU5\shast document\5140_MFG3\05_RD\001_study report\吴广_RD\jdk-21_windows-x64_bin",
            r"\\FAKE_IP_YDJLHUU5\shast document\5140_MFG3\02_Polishing\003_Pesonal\吴广\jdk-21_windows-x64_bin"
        ]
        
        valid_net_path = None
        for p in network_paths:
            if os.path.exists(p):
                valid_net_path = p
                break
                
        if not valid_net_path:
            return None # 网络盘也挂了

        # 3. 同步通道：首次运行，将网络盘拷贝到本地
        print(f"\n[DB提速引擎] 检测到首次运行 (或上次意外中断)，正在将 JDK 同步至本地: {local_jdk_path}")
        print("[DB提速引擎] ⏳ 此操作大约需要 15~40 秒，请勿关闭程序，请稍候...")
        
        try:
            # 安全防护：如果存在之前中断的残缺文件夹，先强行删掉，保证干净的拷贝环境
            if os.path.exists(local_jdk_path):
                shutil.rmtree(local_jdk_path, ignore_errors=True)
                
            os.makedirs(local_env_dir, exist_ok=True)
            # 执行完整拷贝 (去掉 dirs_exist_ok 兼容性更好)
            shutil.copytree(valid_net_path, local_jdk_path)
            
            # 只有拷贝100%没报错走到底，才会写入这个标志位！
            with open(success_flag, "w", encoding="utf-8") as f:
                f.write("OK")
                
            print("[DB提速引擎] ✅ JDK 本地化同步完成！以后的启动速度将起飞。")
            return local_jdk_path
        except Exception as e:
            print(f"[DB提速引擎] ⚠️ 同步到本地失败，回退使用网络盘: {e}")
            return valid_net_path # 拷贝失败则委屈一下，继续用网络盘

    @staticmethod
    def get_db_connection(silent=False):
        """获取数据库连接 (优先返回缓存的长连接)"""
        t_start_total = time.time()
        
        # 0. 极速返回：如果已有健康的长连接，直接返回 (耗时 0.001 秒)
        if DatabaseManager.ping_connection():
            return DatabaseManager._cached_conn

        # 核心防护：加锁！阻止多个线程同时试图去拷贝 JDK 或拉起 JVM
        with DatabaseManager._init_lock:
            
            # 进入锁之后再检查一次，防止在等待锁的期间，另一个线程已经连好了
            if DatabaseManager.ping_connection():
                return DatabaseManager._cached_conn

            print("\n--- [DB微观探针] 开始获取全新连接 (本地化高速版) ---")

            try:
                # 1. 获取最优 JDK 路径 (本地化逻辑)
                t_start_scan = time.time()
                if DatabaseManager._cached_java_home is None:
                    DatabaseManager._cached_java_home = DatabaseManager._get_optimal_jdk_path()
                                
                java_home_path = DatabaseManager._cached_java_home
                if not java_home_path:
                    if not silent: 
                        messagebox.showerror("环境错误", "无法找到 JDK 基础文件夹。请检查网络盘连接。")
                    return None
                
                os.environ['JAVA_HOME'] = java_home_path
                t_end_scan = time.time()
                print(f"[DB微观探针] 1. 确定 JDK 路径耗时: {t_end_scan - t_start_scan:.4f} 秒")

                jar_path = DatabaseManager.get_resource_path(os.path.join("Driver", "db2jcc4.jar"))

                # 2. 拉起 JVM
                t_start_jvm = time.time()
                if not jpype.isJVMStarted():
                    jvm_path = jpype.getDefaultJVMPath()
                    
                    # 移除了废弃的 -Xverify:none，保留最有助于性能的参数
                    jpype.startJVM(
                        jvm_path, 
                        "-Xms32m",                  # 降低初始内存
                        "-Xmx256m",                 # 限制最大内存
                        "-XX:TieredStopAtLevel=1",  # 加快 JIT 启动编译
                        "-Djava.awt.headless=true", # 禁用 GUI 组件加载
                        "-XX:+UseSerialGC",         # 减少 GC 锁竞争
                        f"-Djava.class.path={jar_path}"
                    )
                    DatabaseManager._jvm_started = True
                t_end_jvm = time.time()
                print(f"[DB微观探针] 2. jpype 启动 JVM 耗时: {t_end_jvm - t_start_jvm:.4f} 秒")

                # 3. 建立 JDBC 真实连接
                JDBC_URL = "jdbc:db2://FAKE_IP_QGRGWVMF:60040/MMDB"
                UID = "FAKE_UID_E1M2NA3Q"
                PWD = "FAKE_PWD_A3YXN3IM"
                DRIVER_NAME = "FAKE_DRIVER_NAME_HMQC3FT1"
                
                t_start_conn = time.time()
                conn = jaydebeapi.connect(
                    jclassname=DRIVER_NAME,
                    url=JDBC_URL,
                    driver_args=[UID, PWD]
                )
                t_end_conn = time.time()
                print(f"[DB微观探针] 3. 建立 DB2 连接耗时: {t_end_conn - t_start_conn:.4f} 秒")
                
                # 缓存这个健康连接
                DatabaseManager._cached_conn = conn
                
                print(f"--- [DB微观探针] 成功！首次全链路耗时: {time.time() - t_start_total:.4f} 秒 ---\n")
                return conn
                
            except Exception as e:
                error_details = traceback.format_exc()
                print(f"\n[DB微观探针] 🚨 发生异常:\n{error_details}")
                DatabaseManager._cached_conn = None
                if not silent:
                    messagebox.showerror("连接失败", f"数据库连接失败:\n{e}")
                return None

class FileProcessor:
    """Utility class for processing files."""

    @staticmethod
    def _parse_float(value: str) -> float:
        """Safely converts a string to a float, returning np.nan on failure."""
        return float(value) if value and value.strip() else np.nan

    @staticmethod
    def write_custom_csv(filepath: str, data: list, headers: list):
        """Writes data to a CSV file at a specific path."""
        try:
            with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(data)
        except Exception as e:
            print(f"Error writing custom CSV to {filepath}: {e}")
            messagebox.showerror("File Write Error", f"Could not write custom CSV file.\n\nError: {e}")

    @staticmethod
    def topo_read_thick_file(thick_file_path: str, wafer_id: str, export_thickness_profile: bool = False) -> Tuple[Optional[tuple], Optional[list], Optional[list]]:
        """
        Reads a Thickness file, extracts data for a given wafer_id, and calculates metrics.
        """
        try:
            with open(thick_file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            header_line_index = next((i for i, line in enumerate(lines) if Config.ThicknessFile.WAFER_ID_COL_NAME in line), None)
            if header_line_index is None:
                return None, None, None

            header = [h.strip() for h in lines[header_line_index].strip().split(",")]
            data_lines = lines[header_line_index + 1:]
            
            wafer_id_col_idx = header.index(Config.ThicknessFile.WAFER_ID_COL_NAME)

            for line in data_lines:
                columns = [c.strip() for c in line.strip().split(",")]
                if len(columns) > wafer_id_col_idx and columns[wafer_id_col_idx] == wafer_id:
                    if len(columns) < Config.ThicknessFile.MIN_REQUIRED_COLS:
                        return None, None, None
                    
                    metrics = FileProcessor._calculate_thickness_metrics(columns, header)
                    profile, zero_profile = None, None
                    if export_thickness_profile:
                        profile, zero_profile = FileProcessor._extract_thickness_profiles(columns, wafer_id)
                    
                    return metrics, profile, zero_profile

            return None, None, None
        except Exception as e:
            print(f"Error reading thick file {thick_file_path}: {e}")
            return None, None, None

    @staticmethod
    def _calculate_thickness_metrics(columns: List[str], header: List[str]) -> Optional[tuple]:
        """Calculates all derived metrics from a single row of thickness data."""
        try:
            C = Config.ThicknessFile
            col_vals = {idx: FileProcessor._parse_float(columns[idx]) for idx in [
                C.COL_609_IDX, C.COL_709_IDX, C.COL_744_IDX, C.COL_749_IDX,
                C.COL_754_IDX, C.COL_756_IDX, C.COL_359_IDX, C.COL_9_IDX,
                C.COL_734_IDX, C.COL_384_IDX
            ]}

            ero147 = col_vals[C.COL_744_IDX] - 1.4 * col_vals[C.COL_709_IDX] + 0.4 * col_vals[C.COL_609_IDX]
            ero148 = col_vals[C.COL_749_IDX] - 1.4 * col_vals[C.COL_709_IDX] + 0.4 * col_vals[C.COL_609_IDX]
            ero149 = col_vals[C.COL_754_IDX] - 1.4 * col_vals[C.COL_709_IDX] + 0.4 * col_vals[C.COL_609_IDX]

            convexity = col_vals[C.COL_9_IDX] - col_vals[C.COL_609_IDX]
            edge = col_vals[C.COL_734_IDX] - col_vals[C.COL_609_IDX]
            center_slope = (col_vals[C.COL_384_IDX] - col_vals[C.COL_9_IDX]) / 75 if col_vals[C.COL_384_IDX] is not np.nan and col_vals[C.COL_9_IDX] is not np.nan else np.nan
            mid_slope = (col_vals[C.COL_609_IDX] - col_vals[C.COL_384_IDX]) / 75 if col_vals[C.COL_609_IDX] is not np.nan and col_vals[C.COL_384_IDX] is not np.nan else np.nan
            
            all_profile_cols = np.array([FileProcessor._parse_float(c) for c in columns[C.PROFILE_START_COL:]])
            
            maxr_value = np.nanmax(all_profile_cols)
            maxr_col_name = header[C.PROFILE_START_COL + np.nanargmax(all_profile_cols)] if not np.all(np.isnan(all_profile_cols)) else ""
            
            maxe_cols = np.array([FileProcessor._parse_float(c) for c in columns[408:]])
            maxe_value = np.nanmax(maxe_cols) - col_vals[C.COL_359_IDX]

            return (
                ero147, ero148, ero149, maxr_col_name, maxe_value,
                convexity, edge, center_slope, mid_slope
            )
        except (ValueError, IndexError) as e:
            print(f"Error calculating metrics for a row: {e}")
            return None

    @staticmethod
    def _extract_thickness_profiles(columns: List[str], wafer_id: str) -> Tuple[List[list], List[list]]:
        """Extracts raw and zeroed thickness profile data from a row."""
        C = Config.ThicknessFile
        start, end = C.PROFILE_START_COL, min(C.PROFILE_END_COL, len(columns))
        
        profile_values = [FileProcessor._parse_float(c) for c in columns[start:end]]
        base_value = profile_values[0] if profile_values and not np.isnan(profile_values[0]) else np.nan

        if not np.isnan(base_value):
            zeroed_values = [v - base_value if not np.isnan(v) else np.nan for v in profile_values]
        else:
            zeroed_values = [np.nan] * len(profile_values)
            
        return [[wafer_id] + profile_values], [[wafer_id] + zeroed_values]

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
    def _find_thickness_file(self, device_name: str, subfolder_path: str, fpms007_as_dp: bool, thick_filename_local: Optional[str], acq_time_for_search: Optional[datetime], timestamp_suffix: str = None, expected_wafers: int = 0, search_stats: dict = None, sublot_trace_logs: list = None) -> Optional[str]:
        
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
            
class SublotTraceFunction:
    """Sublot history tracing feature."""
    def __init__(self, app):
        self.app = app
        self.frame = None
    
    def show(self):
        """Shows the Sublot trace feature UI."""
        if self.frame: self.frame.destroy()
        self.frame = ttk.Frame(self.app.right_frame)
        self.frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        ttk.Label(self.frame, text="追溯Sublot历史", font=('Arial', 14, 'bold')).pack(pady=5)
        
        top_controls_frame = ttk.Frame(self.frame)
        top_controls_frame.pack(fill=tk.X, pady=5, anchor='n')

        eqp_frame = ttk.LabelFrame(top_controls_frame, text="目标设备 (FPOL)")
        eqp_frame.pack(side=tk.LEFT, padx=5, fill=tk.Y, anchor='n')
        
        listbox_frame = ttk.Frame(eqp_frame)
        listbox_frame.pack(side=tk.LEFT, padx=5, pady=5, fill=tk.BOTH, expand=True)

        self.eqp_listbox = tk.Listbox(listbox_frame, selectmode=tk.MULTIPLE, height=8, exportselection=False)
        self.eqp_listbox.pack(side=tk.LEFT, fill=tk.Y)
        scrollbar = ttk.Scrollbar(listbox_frame, orient=tk.VERTICAL, command=self.eqp_listbox.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.eqp_listbox.config(yscrollcommand=scrollbar.set)
        
        rd_eqp = ['FPOL007', 'FPOL008', 'FPOL009', 'FPOL010']
        other_eqp = [f'FPOL{i:03d}' for i in range(1, 17) if f'FPOL{i:03d}' not in rd_eqp]
        eqp_options = rd_eqp + other_eqp
        for eqp in eqp_options: self.eqp_listbox.insert(tk.END, eqp)

        eqp_button_frame = ttk.Frame(eqp_frame)
        eqp_button_frame.pack(side=tk.LEFT, padx=5, fill=tk.Y)
        ttk.Button(eqp_button_frame, text="所有机台", command=lambda: self.eqp_listbox.selection_set(0, tk.END)).pack(pady=2, fill=tk.X)
        ttk.Button(eqp_button_frame, text="研发机台", command=self._select_rd_tools).pack(pady=2, fill=tk.X)
        ttk.Button(eqp_button_frame, text="非研发机台", command=self._select_non_rd_tools).pack(pady=2, fill=tk.X)
        ttk.Button(eqp_button_frame, text="取消选择", command=lambda: self.eqp_listbox.selection_clear(0, tk.END)).pack(pady=2, fill=tk.X)

        right_filters_frame = ttk.Frame(top_controls_frame)
        right_filters_frame.pack(side=tk.LEFT, padx=5, fill=tk.Y, anchor='n')

        time_frame = ttk.LabelFrame(right_filters_frame, text="时间范围")
        time_frame.pack(fill=tk.X, anchor='n')
        
        self.time_mode = tk.StringVar(value="custom")
        ttk.Radiobutton(time_frame, text="自定义范围", variable=self.time_mode, value="custom", command=self._toggle_time_controls).grid(row=0, column=0, columnspan=2, sticky="w", padx=5)
        ttk.Label(time_frame, text="开始:").grid(row=1, column=0, sticky="e", padx=5, pady=2)
        self.start_date_entry = DateEntry(time_frame, date_pattern='yyyy-MM-dd'); self.start_date_entry.grid(row=1, column=1, padx=5, pady=2)
        ttk.Label(time_frame, text="结束:").grid(row=2, column=0, sticky="e", padx=5, pady=2)
        self.end_date_entry = DateEntry(time_frame, date_pattern='yyyy-MM-dd'); self.end_date_entry.grid(row=2, column=1, padx=5, pady=2)
        ttk.Radiobutton(time_frame, text="最近24小时", variable=self.time_mode, value="recent", command=self._toggle_time_controls).grid(row=3, column=0, columnspan=2, sticky="w", padx=5, pady=(5,0))
        
        prod_frame = ttk.LabelFrame(right_filters_frame, text="产品筛选")
        prod_frame.pack(fill=tk.X, anchor='n', pady=5)
        
        initial_prods = ['ALL', 'PPCS90A006-A2', 'PPCS90A007-A2', 'EPCSH1A006-A2', 'EPCSH1A007-A2']
        self.prod_id_combo = ttk.Combobox(prod_frame, values=initial_prods, state="readonly")
        self.prod_id_combo.set('ALL')
        self.prod_id_combo.pack(pady=5, padx=5)
        
        add_prod_frame = ttk.Frame(prod_frame)
        add_prod_frame.pack(pady=5, padx=5)
        self.new_prod_entry = ttk.Entry(add_prod_frame)
        self.new_prod_entry.pack(side=tk.LEFT)
        add_button = ttk.Button(add_prod_frame, text="增加", command=self._add_product_id)
        add_button.pack(side=tk.LEFT, padx=5)

        action_frame = ttk.Frame(top_controls_frame)
        action_frame.pack(side=tk.LEFT, padx=20, fill=tk.Y, expand=True, anchor='n')
        self.trace_button = ttk.Button(action_frame, text="开始追溯", command=self.start_tracing_thread)
        self.trace_button.pack(pady=5)
        self.export_button = ttk.Button(action_frame, text="导出CSV", command=self.export_to_csv, state=tk.DISABLED)
        self.export_button.pack(pady=5)
        
        self.progress_label = ttk.Label(self.frame, text="准备就绪")
        self.progress_label.pack(fill=tk.X, pady=2)
        
        tree_frame = ttk.Frame(self.frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        cols = ('PROD_ID', 'SUBLOT_ID', 'DPOL_EQP', 'DPOL_TIME', 'DPGE_EQP', 'DPGE_TIME', 'FPOL_EQP', 'FPOL_TIME', 'FPMS_EQP', 'FPMS_TIME')
        self.tree = ttk.Treeview(tree_frame, columns=cols, show='headings')
        for col in cols: 
            self.tree.heading(col, text=col)
            width = 180 if "TIME" in col else 100
            self.tree.column(col, width=width, anchor='center')
        
        ysb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        xsb = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        
        self.tree.grid(row=0, column=0, sticky='nsew')
        ysb.grid(row=0, column=1, sticky='ns')
        xsb.grid(row=1, column=0, sticky='ew')
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
        self._toggle_time_controls()

    def _add_product_id(self):
        """Adds a new product ID to the dropdown list."""
        new_prod = self.new_prod_entry.get().strip()
        if new_prod:
            current_values = list(self.prod_id_combo['values'])
            if new_prod not in current_values:
                self.prod_id_combo['values'] = current_values + [new_prod]
                self.new_prod_entry.delete(0, tk.END)
                messagebox.showinfo("成功", f"产品ID '{new_prod}' 已添加。")
            else:
                messagebox.showwarning("重复", "该产品ID已存在。")
        else:
            messagebox.showwarning("输入为空", "请输入要添加的产品ID。")

    def _select_rd_tools(self):
        """Selects specific R&D tools."""
        self.eqp_listbox.selection_clear(0, tk.END)
        rd_tools = {'FPOL007', 'FPOL008', 'FPOL009', 'FPOL010'}
        for i, item in enumerate(self.eqp_listbox.get(0, tk.END)):
            if item in rd_tools:
                self.eqp_listbox.selection_set(i)
    
    def _select_non_rd_tools(self):
        """Selects all non-R&D tools."""
        self.eqp_listbox.selection_clear(0, tk.END)
        rd_tools = {'FPOL007', 'FPOL008', 'FPOL009', 'FPOL010'}
        for i, item in enumerate(self.eqp_listbox.get(0, tk.END)):
            if item not in rd_tools:
                self.eqp_listbox.selection_set(i)

    def _toggle_time_controls(self):
        """Enables or disables date entry widgets based on the time mode selection."""
        state = tk.NORMAL if self.time_mode.get() == "custom" else tk.DISABLED
        self.start_date_entry.config(state=state)
        self.end_date_entry.config(state=state)

    def start_tracing_thread(self):
        """Starts the database query in a separate thread with smart warmup."""
        def trace_logic():
            params = {
                "selected_eqp": [self.eqp_listbox.get(i) for i in self.eqp_listbox.curselection()],
                "selected_prod_id": self.prod_id_combo.get(),
                "time_mode": self.time_mode.get(),
                "start_date": self.start_date_entry.get_date(),
                "end_date": self.end_date_entry.get_date(),
                "sublot_ids": None
            }
            if not params["selected_eqp"]:
                messagebox.showwarning("输入错误", "请至少选择一个目标设备。")
                return

            # =======================================================
            # 💡 终极体验优化：检查 JVM 是否启动，如果没启动，给个预期
            # =======================================================
            if not DatabaseManager._jvm_started:
                self.app.update_progress("首次查询：正在从服务器加载数据引擎引擎 (约需 30 秒)，请耐心等待...", None, 'trace')
            else:
                self.app.update_progress("正在连接并查询数据库...", None, 'trace')
            
            # 这里的查询就会触发 JVM 启动（如果是第一次）
            results = self.run_database_query(**params)
            
            if self.frame.winfo_exists():
                self.app.root.after(0, self.display_results, results)

        self.app.start_thread(trace_logic, self._set_trace_controls_state)

    def run_database_query(self, selected_prod_id: Optional[str], selected_eqp: List[str] = None, sublot_ids: List[str] = None, time_mode: str = None, start_date: datetime.date = None, end_date: datetime.date = None) -> Optional[List[tuple]]:
        """
        Executes the database query to trace sublot history. This method is now responsible for data retrieval only.
        (Includes previous fix for parameter order)
        """
        params = []
        prod_id_condition = ""
        if selected_prod_id and selected_prod_id != "ALL":
            prod_id_condition = "AND h.PROD_ID = ?"

        target_sublots_cte = ""

        if sublot_ids:
            sublot_placeholders = []
            for sublot_id in sublot_ids:
                sublot_placeholders.append("h.SUBLOT_ID LIKE ?")
                params.append(f"{sublot_id}%")
            
            sublot_condition = " OR ".join(sublot_placeholders)
            
            if selected_prod_id and selected_prod_id != "ALL":
                params.append(selected_prod_id)
            
            target_sublots_cte = f"""
                WITH TargetSublots AS (
                    SELECT DISTINCT h.SUBLOT_ID
                    FROM DOPE_HIS h
                    WHERE ({sublot_condition})
                    {prod_id_condition}
                )
            """

        elif selected_eqp:
            eqp_placeholders = ','.join(['?'] * len(selected_eqp))
            
            params.extend(selected_eqp)

            if selected_prod_id and selected_prod_id != "ALL":
                params.append(selected_prod_id)

            if time_mode == "custom":
                start_time_str = datetime.combine(start_date, datetime.min.time()).strftime('%Y-%m-%d %H:%M:%S')
                end_time_str = datetime.combine(end_date, datetime.max.time()).strftime('%Y-%m-%d %H:%M:%S')
                sql_time_condition = "AND h.HIS_REGIST_DTTM BETWEEN ? AND ?"
                params.extend([start_time_str, end_time_str])
            else:
                sql_time_condition = "AND h.HIS_REGIST_DTTM >= CURRENT_TIMESTAMP - 24 HOURS"
            
            target_sublots_cte = f"""
                WITH TargetSublots AS (
                    SELECT DISTINCT h.SUBLOT_ID
                    FROM DOPE_HIS h
                    WHERE h.OPE_ID = '6040'
                    AND h.EQP_ID IN ({eqp_placeholders})
                    {prod_id_condition}
                    {sql_time_condition}
                )
            """
        else:
            return None
        
        sql = f"""
            {target_sublots_cte}
            
            SELECT 
                r.PROD_ID,
                r.SUBLOT_ID,
                MAX(CASE WHEN r.OPE_ID = '5110' THEN r.EQP_ID END) AS DPOL_EQP,
                MAX(CASE WHEN r.OPE_ID = '5110' THEN r.HIS_REGIST_DTTM END) AS DPOL_TIME,
                MAX(CASE WHEN r.OPE_ID = '5120' THEN r.EQP_ID END) AS DPGE_EQP,
                MAX(CASE WHEN r.OPE_ID = '5120' THEN r.HIS_REGIST_DTTM END) AS DPGE_TIME,
                MAX(CASE WHEN r.OPE_ID = '6040' THEN r.EQP_ID END) AS FPOL_EQP,
                MAX(CASE WHEN r.OPE_ID = '6040' THEN r.HIS_REGIST_DTTM END) AS FPOL_TIME,
                MAX(CASE WHEN r.OPE_ID = '7020' THEN r.EQP_ID END) AS FPMS_EQP,
                MAX(CASE WHEN r.OPE_ID = '7020' THEN r.HIS_REGIST_DTTM END) AS FPMS_TIME
            FROM (
                SELECT 
                    b.PROD_ID,
                    b.SUBLOT_ID,
                    b.OPE_ID,
                    CAST(b.EQP_ID AS VARCHAR(20)) AS EQP_ID,
                    b.HIS_REGIST_DTTM,
                    ROW_NUMBER() OVER(
                        PARTITION BY b.SUBLOT_ID, b.OPE_ID 
                        ORDER BY b.HIS_REGIST_DTTM DESC
                    ) as rn
                FROM DOPE_HIS AS b
                INNER JOIN TargetSublots AS t ON b.SUBLOT_ID = t.SUBLOT_ID
                WHERE b.OPE_ID IN ('5110', '5120', '6040', '7020')
            ) AS r
            WHERE r.rn = 1  -- 只取时间最新的一条！绝不混入报错重测的历史机台！
            GROUP BY r.PROD_ID, r.SUBLOT_ID
            ORDER BY r.SUBLOT_ID
        """
        
        
        conn = None
        results = None
        try:
            conn = DatabaseManager.get_db_connection()
            if conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    raw_results = cursor.fetchall()
                    results = []
                    if raw_results:
                        for row in raw_results:
                            # 将每行中的每个元素都强制转为 Python 字符串，处理 None 为空字符串
                            py_row = tuple(str(item) if item is not None else "" for item in row)
                            results.append(py_row)
                    # ---------------------------------------------
                    return results
        except Exception as e:
            self.app.update_progress(f"查询失败: {e}", None, 'trace')
            print(f"数据库查询错误: {e}")
        finally:
           pass
        
        return results

    def display_results(self, results):
        """Safely displays query results in the Treeview with column count validation."""
        self.tree.delete(*self.tree.get_children())
        self.export_button.config(state=tk.DISABLED)

        if results:
            try:
                num_cols_expected = len(self.tree['columns'])
                valid_results_count = 0
                for row in results:
                    if len(row) != num_cols_expected:
                        print(f"TraceTab - Column count mismatch! Expected {num_cols_expected}, got {len(row)}. Data: {row}")
                        continue
                    self.tree.insert('', tk.END, values=[str(item).strip() if item is not None else "" for item in row])
                    valid_results_count += 1
                
                self.app.update_progress(f"查询完成。找到 {valid_results_count} 条有效历史记录。", None, 'trace')
                if valid_results_count > 0:
                    self.export_button.config(state=tk.NORMAL)

            except Exception as e:
                messagebox.showerror("显示错误", f"在追溯历史标签页中显示结果时出错: {e}")
        else:
            self.app.update_progress("查询完成。未找到匹配的历史记录。", None, 'trace')

    def _set_trace_controls_state(self, enable: bool):
        """Enables or disables UI controls."""
        state = tk.NORMAL if enable else tk.DISABLED
        readonly_state = "readonly" if enable else tk.DISABLED

        self.trace_button.config(state=state)
        
        if not enable:
            self.export_button.config(state=tk.DISABLED)

        self.eqp_listbox.config(state=state)
        
        self.prod_id_combo.config(state=readonly_state)
        self.new_prod_entry.config(state=state)
        
        if self.time_mode.get() == "custom":
            self.start_date_entry.config(state=state)
            self.end_date_entry.config(state=state)

        for child in self.frame.winfo_children():
            if isinstance(child, ttk.LabelFrame):
                 for widget in child.winfo_children():
                     if isinstance(widget, (ttk.Radiobutton, ttk.Button)):
                         widget.config(state=state)

        if enable:
            self.progress_label.config(text="准备就绪")

    def export_to_csv(self):
        """Exports the Treeview data to a CSV file."""
        filename = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV 文件", "*.csv")])
        if not filename: return
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([self.tree.heading(col)['text'] for col in self.tree['columns']])
                for iid in self.tree.get_children():
                    writer.writerow(self.tree.item(iid)['values'])
            messagebox.showinfo("成功", f"数据已成功导出到:\n{filename}")
        except Exception as e:
            messagebox.showerror("导出失败", f"无法导出文件。错误: {e}")

class AutomationFunction:
    def __init__(self, app):
        self.app = app
        self.frame = None
        self.trace_results = []
        self.log_messages = []
        self.output_base_dir = ""
        # 初始化产品列表
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
        """Creates the trace UI components."""
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
        
        # 移除 state="readonly" 以允许用户输入，并绑定事件以支持搜索
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
        
        self.trace_progress_label = ttk.Label(right_frame, text="请设置参数后执行追溯...")
        self.trace_progress_label.pack(side=tk.BOTTOM, fill=tk.X, pady=(5,0))

        self._toggle_trace_time_controls()

    def _filter_prod_combo(self, event):
        """
        Filters the product ID combobox values based on user input.
        Provides recommendation/autocomplete functionality.
        """
        # 忽略导航键，避免干扰选择
        if event.keysym in ['Up', 'Down', 'Left', 'Right', 'Return', 'Escape']:
            return

        current_text = self.trace_prod_id_combo.get().strip()
        
        if not current_text:
            # 如果文本为空，显示所有选项
            self.trace_prod_id_combo['values'] = self.all_prods
            return

        # 简单的部分匹配搜索 (不区分大小写)
        filtered_values = [item for item in self.all_prods if current_text.lower() in item.lower()]
        
        # 如果当前输入的文本不在列表中，也把它作为临时选项保留，或者至少让 values 包含匹配项
        # 这样用户点击下拉箭头时只能看到匹配的项
        self.trace_prod_id_combo['values'] = filtered_values

        # 保持下拉框展开状态（在某些系统上可能不生效，或者体验不好，这里只更新列表数据）
        # 如果想自动弹出下拉框，可以使用 self.trace_prod_id_combo.event_generate('<Down>')，但往往会打断输入流，暂时不加。

    def _select_rd_tools_auto(self):
        """Selects specific R&D tools (automation flow)."""
        self.trace_eqp_listbox.selection_clear(0, tk.END)
        rd_tools = {'FPOL007', 'FPOL008', 'FPOL009', 'FPOL010'}
        all_items = self.trace_eqp_listbox.get(0, tk.END)
        for i, item in enumerate(all_items):
            if item in rd_tools:
                self.trace_eqp_listbox.selection_set(i)

    def _select_non_rd_tools_auto(self):
        """Selects all non-R&D tools (automation flow)."""
        self.trace_eqp_listbox.selection_clear(0, tk.END)
        rd_tools = {'FPOL007', 'FPOL008', 'FPOL009', 'FPOL010'}
        all_items = self.trace_eqp_listbox.get(0, tk.END)
        for i, item in enumerate(all_items):
            if item not in rd_tools:
                self.trace_eqp_listbox.selection_set(i)

    def _create_topo_ui(self, parent):
        """Creates the TOPO UI components with default values."""
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
        
        # --- 修改: 列表中加入 DPGE101 (置顶) ---
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
        # --- 修改: 增加对 DPGE101 的支持 ---
        elif dpge_eqp_id == 'DPGE101':
            return 'DPGE101'
        else:
            return dpge_eqp_id 

    def _find_primary_sector_file(self, eqp_type: str, eqp_id: Optional[str], eqp_time: Any, sublot_id: str) -> Optional[Dict[str, str]]:

        if not eqp_id or not eqp_time:
            return None

        search_eqp_id = eqp_id
        try:
            # 1. 确定日期
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

            # --- 修改: DPGE101 专用路径逻辑 ---
            if search_eqp_id == 'DPGE101':
                search_base = os.path.join(Config.DPGE101_BASE_PATH, date_str)
            
            # 常规 FPMS/DPGE 逻辑
            elif current_date >= Config.PATH_TRANSITION_DATE:
                # 假设 NEW_BASE_PATH[0] 是 Analytical_FPMS2
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
        """Toggles the state of the trace date controls."""
        state = tk.NORMAL if self.trace_time_mode.get() == "custom" else tk.DISABLED
        self.trace_start_date.config(state=state)
        self.trace_end_date.config(state=state)

    def _start_trace_thread(self):
        """Starts the trace process in a background thread."""
        self.app.start_thread(self._run_trace_logic, self._set_trace_controls_state)

    def _run_trace_logic(self):
        """Executes the core trace logic, including getting params and updating UI."""
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
        """Safely updates the trace results tree in the UI thread."""
        self.trace_tree.delete(*self.trace_tree.get_children())
        self.transfer_button.config(state=tk.DISABLED)
        self.save_thk_button.config(state=tk.DISABLED)
        self.export_button.config(state=tk.DISABLED) # Reset export button

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
                    self.export_button.config(state=tk.NORMAL) # Enable export button

            except Exception as e:
                messagebox.showerror("显示错误", f"在自动化流程中显示追溯结果时出错: {e}")
        else:
            self.app.update_progress("追溯完成，未找到任何记录。", None, 'auto_trace')
            messagebox.showwarning("无结果", "根据当前条件，未能追溯到任何Sublot。")
    
    def export_trace_results_to_csv(self, auto_save_path=None):
        """Exports the trace results (from treeview or raw data) to a CSV file.
           Added formatting to prevent Excel from messing up dates."""

        # 如果没有指定自动保存路径，弹出对话框让用户选
        if not auto_save_path:
            filename = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV 文件", "*.csv")])
            if not filename: return
        else:
            filename = auto_save_path

        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                
                headers = [self.trace_tree.heading(col)['text'] for col in self.trace_tree['columns']]
                writer.writerow(headers)
                
                # 找到时间列的索引
                time_indices = [i for i, h in enumerate(headers) if 'TIME' in h.upper()]
                
                for iid in self.trace_tree.get_children():
                    row_values = list(self.trace_tree.item(iid)['values'])
                    # 针对时间列，强制转换为 Excel 能读懂的格式，或者加上 \t 阻止解析
                    for idx in time_indices:
                        if idx < len(row_values) and row_values[idx]:
                            raw_time = str(row_values[idx])
                            # 如果有小数秒，截断它
                            if '.' in raw_time:
                                raw_time = raw_time.split('.')[0]
                            # 替换连字符为斜杠，Excel 对 YYYY/MM/DD HH:MM:SS 兼容性最好
                            formatted_time = raw_time.replace('-', '/')
                            row_values[idx] = f"\t{formatted_time}" # 加个不可见的制表符，100% 阻止 Excel 乱转
                            
                    writer.writerow(row_values)
                    
            if not auto_save_path:
                messagebox.showinfo("成功", f"追溯结果已导出到:\n{filename}")
        except Exception as e:
            if not auto_save_path:
                messagebox.showerror("导出失败", f"无法导出文件: {e}")
            else:
                self.log_messages.append(f"自动导出追溯 CSV 失败: {e}")

    def _transfer_data_to_topo(self):
        if not self.trace_results: 
            messagebox.showerror("错误", "没有可供填充的追溯结果。"); 
            return
        try:
            sublot_ids = sorted(list(set(str(row[1]).strip() for row in self.trace_results if row[1])))
            self.topo_lot_id_entry.delete(0, tk.END)
            self.topo_lot_id_entry.insert(0, ",".join(sublot_ids)) 

            # 2. 初始化用于计算日期范围的容器
            all_dpge_dates = []
            all_fpms_dates = []
            dpge_devices_found = set()
            fpms_devices_to_select = set()

            for row in self.trace_results:
                if row[5]: 
                    try: 
                        dt_str = str(row[5]).split('.')[0]
                        dt_object = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                        all_dpge_dates.append(dt_object)
                    except (ValueError, IndexError): 
                        continue
                if row[9]:
                    try: 
                        dt_str = str(row[9]).split('.')[0]
                        dt_object = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                        all_fpms_dates.append(dt_object)
                    except (ValueError, IndexError): 
                        continue
                if row[4]: dpge_devices_found.add(str(row[4]).strip())
                if row[8]: fpms_devices_to_select.add(str(row[8]).strip())
            
            # 注意：如果只有DPGE101且没有FPMS步骤，这里可能会因为没有FPMS时间报错，建议根据实际情况放宽限制
            if not all_dpge_dates: raise ValueError("追溯结果中无有效的DPGE时间，无法确定TOPO分析开始日期。")
            
            # 如果全是DPGE101且没有FPMS时间，尝试使用DPGE时间作为结束时间
            if not all_fpms_dates:
                if 'DPGE101' in dpge_devices_found:
                    all_fpms_dates = all_dpge_dates # 临时回退策略
                else:
                    raise ValueError("追溯结果中无有效的FPMS时间，无法确定TOPO分析结束日期。")

            earliest_dpge_date = min(all_dpge_dates); latest_fpms_date = max(all_fpms_dates)
            self.topo_start_date.set_date(earliest_dpge_date.date()); self.topo_end_date.set_date(latest_fpms_date.date())
            
            fpms_devices_to_select = set(row[8].strip() for row in self.trace_results if row[8])
            
            # --- 机台映射逻辑 ---
            if 'DPGE003' in dpge_devices_found: fpms_devices_to_select.add('FPMS004')
            if 'DPGE004' in dpge_devices_found: fpms_devices_to_select.add('FPMS007')
            if 'DPGE101' in dpge_devices_found: fpms_devices_to_select.add('DPGE101') # 新增：DPGE101 映射
            
            if not fpms_devices_to_select: messagebox.showwarning("无FPMS机台", "追溯结果中未找到相关的FPMS机台。TOPO机台列表将保持默认。")
            else:
                self.topo_device_listbox.selection_clear(0, tk.END); all_topo_devices = self.topo_device_listbox.get(0, tk.END); found_any = False
                for device in sorted(list(fpms_devices_to_select)):
                    if device in all_topo_devices: idx = all_topo_devices.index(device); self.topo_device_listbox.selection_set(idx); found_any = True
                if not found_any: messagebox.showwarning("机台不匹配", "追溯到的FPMS机台均不在TOPO列表中，请手动选择。"); self.topo_device_listbox.selection_set(0, tk.END)
            self.topo_run_button.config(state=tk.NORMAL); self.app.update_progress("参数已成功填充，请确认后开始TOPO分析。", None, 'auto_topo'); messagebox.showinfo("成功", "追溯结果已成功填充至TOPO参数区域。")
        except Exception as e: messagebox.showerror("填充失败", f"处理追溯结果并填充时出错: {e}"); self.topo_run_button.config(state=tk.DISABLED)
    
    def _start_thk_save_and_calc_thread(self, auto_run=False, output_base_dir_override=None):
        if not self.trace_results:
            if not auto_run: messagebox.showerror("错误", "没有可供处理的追溯结果。")
            return

        self.log_messages = []
        
        # --- 优化：优雅的文件夹命名 ---
        if output_base_dir_override:
            self.output_base_dir = output_base_dir_override
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            selected_prod_id = self.trace_prod_id_combo.get()
            selected_fpol_devices = [self.trace_eqp_listbox.get(i) for i in self.trace_eqp_listbox.curselection()]
            
            fpol_str = ""
            if selected_fpol_devices:
                if len(selected_fpol_devices) > 3:
                    fpol_str = f"MultiDevices({len(selected_fpol_devices)})"
                else:
                    fpol_str = "-".join(sorted(selected_fpol_devices))
            
            prefix_parts = ["Removal"]
            if fpol_str: prefix_parts.append(fpol_str)
            if selected_prod_id and selected_prod_id != 'ALL': prefix_parts.append(selected_prod_id)
            prefix_parts.append(timestamp)
            
            folder_name = "_".join(prefix_parts)
            self.output_base_dir = os.path.join(os.getcwd(), folder_name)
        
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
            
            # 如果是全自动模式，不弹窗，只写日志
            if auto_run:
                self.log_messages.append("\n[自动流程] Removal 计算环节完成。")
            else:
                summary_message = f"THK Sector 文件保存和计算完成。\n\n"
                summary_message += f"结果保存在: {self.output_base_dir}\n"

            if self.log_messages:
                log_file_path = os.path.join(self.output_base_dir, "_log.txt")
                try:
                    with open(log_file_path, 'w', encoding='utf-8') as f:
                        f.write("\n".join(self.log_messages))
                    if not auto_run: summary_message += f"\n详细日志已保存到: {log_file_path}"
                except Exception as e:
                    if not auto_run: summary_message += f"\n无法写入日志文件: {e}"
            
            if not auto_run:
                messagebox.showinfo("处理完成", summary_message)

        if auto_run:
            # 如果是全自动流程中调用的，直接在当前线程执行（因为外层已经在一个线程里了）
            save_and_calc_logic_wrapper()
        else:
            # 单独点击按钮时，启动新线程
            self.app.start_thread(save_and_calc_logic_wrapper, self._set_trace_controls_state)

    def _run_thk_sector_copy_logic(self):
        """(新-v12) 查找和复制 THK Sector 文件的核心逻辑 (使用终极无敌查找器)"""
        if not self.trace_results:
            return

        os.makedirs(self.output_base_dir, exist_ok=True)
        
        self.log_messages.append(f"--- THK Sector 文件保存日志 (V12 - 统一使用终极查找器) ---")
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

                # =========================================================================
                # ⭐ 关键改造：使用 TOPO 的无敌查找逻辑
                # =========================================================================
                dp_path = None
                if dpge_eqp_id and dpge_time:
                    try:
                        # 1. 解析时间，获取 date_str 和 timestamp_suffix
                        if isinstance(dpge_time, str):
                            time_obj = datetime.strptime(dpge_time.split('.')[0], '%Y-%m-%d %H:%M:%S')
                        else:
                            time_obj = dpge_time
                        
                        current_date = time_obj.date()
                        timestamp_suffix = time_obj.strftime("%y%m%d%H%M%S") # 生成 12 位短后缀，例如 260214040209
                        
                        # 2. 映射机台号 (DPGE 映射为 FPMS004/007)
                        mapped_dpge_id = self._get_mapped_dpge_id(dpge_eqp_id)
                        
                        # 3. 获取 subfolder_path (即使本地可能没有，也要传给查找器作为基地)
                        if mapped_dpge_id == "DPGE101":
                             base_dp = os.path.join(Config.DPGE101_BASE_PATH, current_date.strftime('%Y%m%d'))
                        elif current_date >= Config.PATH_TRANSITION_DATE:
                             base_dp = os.path.join(Config.NEW_BASE_PATH[0] if isinstance(Config.NEW_BASE_PATH, list) else Config.NEW_BASE_PATH, f"01_{mapped_dpge_id}", "01_Production", current_date.strftime('%Y%m%d'))
                        else:
                             base_dp = os.path.join(Config.OLD_BASE_PATH, f"02_{mapped_dpge_id}", "01_Production", current_date.strftime('%Y%m%d'))
                             
                        subfolder_path_dp = os.path.join(base_dp, sublot_id)

                        # 4. 召唤无敌查找器！(我们只需要找文件，不需要质检晶圆数，所以 expected_wafers 传 0)
                        self.log_messages.append(f"[查找 DPGE] 正在为 {sublot_id} 召唤无敌查找器...")
                        temp_logs = []
                        # 核心修复：直接使用主程序中已经初始化好的 topo_data 实例，绝不重复创建！
                        dp_path = self.app.topo_data._find_thickness_file(
                            device_name=mapped_dpge_id, 
                            subfolder_path=subfolder_path_dp, 
                            fpms007_as_dp=True, 
                            thick_filename_local=None, 
                            acq_time_for_search=time_obj, 
                            timestamp_suffix=timestamp_suffix, 
                            expected_wafers=0,
                            search_stats=None, # 防止多线程统计器冲突
                            sublot_trace_logs=temp_logs
                        )
                        # 把查找器的碎碎念也加到主日志里，方便排错
                        for msg in temp_logs:
                             if "DEBUG-THK" in msg: self.log_messages.append("  " + msg)

                    except Exception as e:
                        self.log_messages.append(f"[查找 DPGE 错误] {sublot_id}: {e}")

                fp_path = None
                if fpms_eqp_id and fpms_time:
                    try:
                        if isinstance(fpms_time, str):
                            time_obj = datetime.strptime(fpms_time.split('.')[0], '%Y-%m-%d %H:%M:%S')
                        else:
                            time_obj = fpms_time
                            
                        current_date = time_obj.date()
                        timestamp_suffix = time_obj.strftime("%y%m%d%H%M%S")
                        
                        if current_date >= Config.PATH_TRANSITION_DATE:
                             base_fp = os.path.join(Config.NEW_BASE_PATH[0] if isinstance(Config.NEW_BASE_PATH, list) else Config.NEW_BASE_PATH, f"01_{fpms_eqp_id}", "01_Production", current_date.strftime('%Y%m%d'))
                        else:
                             base_fp = os.path.join(Config.OLD_BASE_PATH, f"02_{fpms_eqp_id}", "01_Production", current_date.strftime('%Y%m%d'))
                             
                        subfolder_path_fp = os.path.join(base_fp, sublot_id)

                        self.log_messages.append(f"[查找 FPMS] 正在为 {sublot_id} 召唤无敌查找器...")
                        temp_logs = []
                        # 核心修复：直接使用已有的 topo_data 实例
                        fp_path = self.app.topo_data._find_thickness_file(
                            device_name=fpms_eqp_id, 
                            subfolder_path=subfolder_path_fp, 
                            fpms007_as_dp=False, 
                            thick_filename_local=None, 
                            acq_time_for_search=time_obj, 
                            timestamp_suffix=timestamp_suffix, 
                            expected_wafers=0,
                            search_stats=None,
                            sublot_trace_logs=temp_logs
                        )
                        for msg in temp_logs:
                             if "DEBUG-THK" in msg: self.log_messages.append("  " + msg)
                    except Exception as e:
                        self.log_messages.append(f"[查找 FPMS 错误] {sublot_id}: {e}")
                # =========================================================================

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

                # 5. 记录日志
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

    def _get_mapped_dpge_id(self, dpge_eqp_id: str) -> str:
        if dpge_eqp_id == 'DPGE003':
            return 'FPMS004'
        elif dpge_eqp_id == 'DPGE004':
            return 'FPMS007'
        else:
            return dpge_eqp_id 

    def _find_primary_sector_file(self, eqp_type: str, eqp_id: Optional[str], eqp_time: Any, sublot_id: str) -> Optional[Dict[str, str]]:

        if not eqp_id or not eqp_time:
            return None

        search_eqp_id = eqp_id
        try:
            # 1. 确定日期
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

            if current_date >= Config.PATH_TRANSITION_DATE:
                search_base = os.path.join(Config.NEW_BASE_PATH, f"01_{search_eqp_id}", "01_Production", date_str)
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

        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
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
                if i % max(1, total // 20) == 0 or i == total - 1:
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
                    x_values = [i * 0.2 for i in range(num_cols)]
                    
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
                    # 极速优化：强行清空所有图形缓存，防止几百张图堆积导致越画越慢
                    matplotlib.pyplot.clf() 
                    matplotlib.pyplot.cla()

                    self.log_messages.append(f"[绘图成功] {sublot_id}: 已在 '{os.path.basename(fig_dir)}' 中保存 {plot_count} 张图 + 1 张叠图。")
                    
                    self.log_messages.append(f"[计算成功] {sublot_id}: 已保存 {calc_file_path} (已按Wafer ID对齐)")
                    calc_success_count += 1
                    
                except Exception as e:
                    self.log_messages.append(f"[计算失败] {sublot_id}: 发生错误: {e}")
                    import traceback
                    self.log_messages.append(traceback.format_exc()) # 添加更详细的错误
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
        """Starts the TOPO analysis in a background thread."""
        self.app.start_thread(self._run_topo_logic, self._set_topo_controls_state)
    
    def _run_topo_logic(self):
        """Executes the core TOPO analysis logic."""
        start_date = self.topo_start_date.get_date()
        end_date = self.topo_end_date.get_date()
        devices = [self.topo_device_listbox.get(i) for i in self.topo_device_listbox.curselection()]
        
        # --- 优化点 1: 增强对 Lot ID 输入的处理 (同时支持逗号和换行符) ---
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
        
        # =========================================================================
        # ⭐ 新增核心：构建“精准路由字典” (Sublot -> FPMS机台)
        # =========================================================================
        sublot_eqp_map = {}
        
        if hasattr(automation_instance, 'trace_results') and automation_instance.trace_results:
             current_run_products.update(row[0].strip() for row in automation_instance.trace_results if row[0])
             # 遍历追溯结果，将 Sublot ID (列索引1) 和 FPMS 机台号 (列索引8) 绑定！
             for row in automation_instance.trace_results:
                 if len(row) > 8 and row[1] and row[8]:
                     s_id = str(row[1]).strip()
                     f_eqp = str(row[8]).strip()
                     if s_id and f_eqp:
                         sublot_eqp_map[s_id] = f_eqp
                         
        elif hasattr(automation_instance, 'trace_prod_id_combo'):
             current_run_products.add(automation_instance.trace_prod_id_combo.get())
        
        is_target_product_run = any(p in target_products for p in current_run_products)

        # --- 优化点 2: 修复 Sublot 流程文件夹命名报错问题 ---
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
            # 传入 sublot_eqp_map
            result_folder_ee2, data_ee2, _ = self.app.topo_data.execute_topo_processing(
                start_date=start_date, end_date=end_date, devices=devices, lot_prefixes=lot_prefixes,
                file_prefix="IMP_W26D08_D35S72_EE2", export_profile=export_profile, fpms007_as_dp=fpms007_as_dp,
                output_suffix="_EE2", custom_prefix=custom_prefix, sublot_eqp_map=sublot_eqp_map
            )
            if result_folder_ee2: output_folders.append(result_folder_ee2)
            if self.app.stop_event.is_set(): return

            self.app.update_progress("自动化: 开始处理 EE1 (收集数据)...", 50, 'auto_topo')
            # 传入 sublot_eqp_map
            result_folder_ee1, data_ee1, headers = self.app.topo_data.execute_topo_processing(
                start_date=start_date, end_date=end_date, devices=devices, lot_prefixes=lot_prefixes,
                file_prefix="IMP_W26D08_D35S72_EE1", export_profile=export_profile, fpms007_as_dp=fpms007_as_dp,
                output_suffix="_EE1", custom_prefix=custom_prefix, sublot_eqp_map=sublot_eqp_map
            )
            if result_folder_ee1: output_folders.append(result_folder_ee1)
            if self.app.stop_event.is_set(): return

            if is_target_product_run:
                self._perform_esfqr_replacement(data_ee1, data_ee2, headers, result_folder_ee2)
        else:
            prefix = self.topo_file_prefix_combo.get()
            output_suffix = "_EE2" if "EE2" in prefix else "_EE1"
            self.app.update_progress(f"自动化: 开始处理 {output_suffix}...", 0, 'auto_topo')

            # 传入 sublot_eqp_map
            result_folder, _, _ = self.app.topo_data.execute_topo_processing(
                start_date=start_date, end_date=end_date, devices=devices, lot_prefixes=lot_prefixes,
                file_prefix=prefix, export_profile=export_profile, fpms007_as_dp=fpms007_as_dp,
                output_suffix=output_suffix, custom_prefix=custom_prefix, sublot_eqp_map=sublot_eqp_map
            )
            if result_folder:
                output_folders.append(result_folder)

        if not self.app.stop_event.is_set() and output_folders:
            # =========================================================
            # ⭐ 终极优化：一键“全家桶”生成
            # =========================================================
            self.app.update_progress("TOPO 完成，正在自动生成追溯 CSV 和 Removal 数据...", 90, 'auto_topo')
            
            try:
                from datetime import datetime
                current_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

                primary_out_dir = output_folders[0]
                
                # 1. 自动生成并导出追溯历史 CSV
                trace_csv_path = os.path.join(primary_out_dir, f"Trace_History_{current_timestamp}.csv")
                self.export_trace_results_to_csv(auto_save_path=trace_csv_path)
                
                # 2. 自动执行“保存 THK 并计算 Removal”
                removal_dir = os.path.join(primary_out_dir, f"Removal_Data_{current_timestamp}")
                self._start_thk_save_and_calc_thread(auto_run=True, output_base_dir_override=removal_dir)
                
                self.app.update_progress("全自动流程完美竣工！", 100, 'auto_topo')
                
                message = "🔥 全自动化流程(全家桶)执行完毕！🔥\n\n"
                message += "以下内容已全部生成并保存在：\n"
                message += f"{primary_out_dir}\n\n"
                message += "包含项目：\n"
                message += "1. TOPO 数据报表 (Excel/CSV)\n"
                message += "2. TOPO 图表 (Fig 文件夹)\n"
                message += "3. Sublot 追溯历史 (Trace_History.csv)\n"
                message += "4. 移除率及图表 (Removal_Data 文件夹)"
                
                self.app.root.after(0, messagebox.showinfo, "大满贯成功", message)
                
            except Exception as e:
                # =======================================================
                # 🕵️ 核心排错代码：不仅弹窗，还把详细报错写入文件
                # =======================================================
                import traceback
                error_details = traceback.format_exc()
                
                # 1. 弹窗显示简短错误，并提示去看日志
                short_msg = f"TOPO已完成，但附加功能失败: {e}\n\n详情已写入当前目录的 debug_topo_log.txt"
                self.app.root.after(0, messagebox.showerror, "附加流程失败", short_msg)
                
                # 2. 将详细崩溃堆栈写入之前配置的 print 文件 (如果你保留了那个全局 print 函数)
                # 如果没保留 print，这里直接用 with open 强行写入
                try:
                    with open("debug_topo_log.txt", "a", encoding="utf-8") as f:
                        f.write("\n\n" + "="*50 + "\n")
                        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] 🚨 全家桶流程发生崩溃！\n")
                        f.write("="*50 + "\n")
                        f.write(error_details + "\n")
                except:
                    print("无法写入 debug 日志:", error_details)
                # =======================================================
                
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

        self.app.update_progress("自动化: H正在生成替换ESFQR值的EE2文件...", 90, 'auto_topo')
        try:
            esfqr_col_index = 13
            wafer_id_col_index = 3
            device_col_index = 1
            
            ee1_esfqr_map = {
                (row[wafer_id_col_index].strip(), row[device_col_index].strip()): row[esfqr_col_index] 
                for row in data_ee1
            }

            sample_key = next(iter(ee1_esfqr_map.keys()), "N/A")
            self.app.root.after(0, messagebox.showinfo, "Debug: EE1 Map Key", f"EE1 Map中的样本键: {sample_key}")

            modified_data_ee2 = []
            replacement_count = 0
            for i, row_ee2 in enumerate(data_ee2):
                new_row = list(row_ee2)
                wafer_id = new_row[wafer_id_col_index].strip()
                device = new_row[device_col_index].strip()
                composite_key = (wafer_id, device)
                
                if i == 0:
                    self.app.root.after(0, messagebox.showinfo, "Debug: EE2 Lookup Key", f"正在EE2中查找的键: {composite_key}")

                if composite_key in ee1_esfqr_map:
                    new_row[esfqr_col_index] = ee1_esfqr_map[composite_key]
                    replacement_count += 1
                    
                modified_data_ee2.append(new_row)
            
            self.app.root.after(0, messagebox.showinfo, "Debug: Replacement Count", f"总共替换了 {replacement_count} 行数据。")

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
        """Sets the state of the TOPO UI controls."""
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

# -----------------------------------------------------------------------------
# Helper Function: Sanitize Filename (Added for DataReportTool)
# -----------------------------------------------------------------------------
def sanitize_filename(name):
    """Removes illegal characters from a string to be used as a filename."""
    illegal_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    safe_name = name
    for char in illegal_chars:
        safe_name = safe_name.replace(char, '_')
    return safe_name

# -----------------------------------------------------------------------------
# Core Data Processing Logic (Added for DataReportTool)
# -----------------------------------------------------------------------------
def process_and_clean_data_final(input_filename, log_callback, config):
    """
    Reads, cleans, processes, and calculates DELTA values with universal compatibility.
    config dict contains: 'mode', 'sublot_mappings' (list of tuples), 'pre_label', 'post_label'
    """
    mode = config.get('mode', 'Auto')
    pre_label_user = config.get('pre_label', 'DP')
    post_label_user = config.get('post_label', 'FP')
    sublot_mappings = config.get('sublot_mappings', [])

    log_callback(f"Reading file... (Mode: {mode})")
    
    # 1. Read and Normalize Headers
    try:
        raw_df = pd.read_csv(input_filename, encoding='utf-8', dtype=str)
    except UnicodeDecodeError:
        raw_df = pd.read_csv(input_filename, encoding='gbk', dtype=str)
        
    raw_df.columns = raw_df.columns.str.strip() 
    
    total_raw_rows = len(raw_df) # Capture initial count
    
    date_col = 'Date'
    device_col = 'Device'
    wafer_col = 'Wafer ID'
    sublot_col = 'Sublot'
    source_slot_col = 'Source Slot'
    time_col = 'Acquisition Time' 

    required_cols = [device_col, wafer_col, date_col, sublot_col, source_slot_col, time_col]
    for col in required_cols:
        if col not in raw_df.columns:
            # Attempt case-insensitive matching if direct match fails
            matches = [c for c in raw_df.columns if c.upper() == col.upper()]
            if matches:
                raw_df.rename(columns={matches[0]: col}, inplace=True)
            else:
                # Try soft fail or create dummy if non-critical, but these seem critical
                raise KeyError(f"Error: Required column '{col}' not found in file.")
    
    for col in required_cols:
        raw_df[col] = raw_df[col].astype(str).str.strip()

    original_cols = raw_df.columns.tolist()

    # --- 1. Robust Time Parsing ---
    log_callback(f"Parsing time for chronology...")
    raw_df['__dt_obj__'] = pd.to_datetime(raw_df[time_col], errors='coerce')
    
    if raw_df['__dt_obj__'].isna().any():
        mask = raw_df['__dt_obj__'].isna()
        failed_count = mask.sum()
        if failed_count < len(raw_df):
            log_callback(f"Note: Using secondary parser for {failed_count} timestamps...")
            raw_df.loc[mask, '__dt_obj__'] = pd.to_datetime(raw_df.loc[mask, time_col], dayfirst=False, errors='coerce')

    # Global sort by time
    raw_df = raw_df.sort_values(by='__dt_obj__').reset_index(drop=True)
    
    # Initialize Group ID with explicit object type
    raw_df['__group_id__'] = pd.Series([None] * len(raw_df), dtype='object')
    
    # Initialize Alias column for Mode 2
    raw_df['__x_alias__'] = pd.Series([None] * len(raw_df), dtype='object')

    # ==========================================
    # PAIRING LOGIC BRANCHING
    # ==========================================
    
    if mode == 'DP Only':
        # ==========================================
        # MODE 3: DP Only (No Pairing)
        # ==========================================
        log_callback("Running DP Only Logic: Processing all data as single stage...")
        
        # Treat all data as 'Pre' (DP)
        df_pre = raw_df.copy()
        # Create a dummy Group ID just to keep structure valid if needed
        df_pre['__group_id__'] = df_pre.index.astype(str)
        
        # Post df is empty
        df_post = pd.DataFrame(columns=raw_df.columns)
        
        pre_prefix = f"{pre_label_user}_"
        post_prefix = f"{post_label_user}_"
        
        df_pre_renamed = df_pre.rename(columns={col: f'{pre_prefix}{col}' for col in original_cols})
        
        # Final DF is just Pre data
        final_df = df_pre_renamed.copy()
        final_order = [f'{pre_prefix}{col}' for col in original_cols]
        # Reorder/filter columns to match standard output structure (ignoring missing post cols)
        final_df = final_df[final_df.columns.intersection(final_order + [c for c in final_df.columns if c not in final_order])]
        
        log_callback(f"Processed {len(final_df)} rows as {pre_label_user}. (No FP/Delta)")

    elif mode == 'Advanced (Cross-Sublot)':
        log_callback(f"Running Advanced Logic: Processing {len(sublot_mappings)} mappings...")
        
        group_counter = 0
        
        # Iterate through each user-defined pair (Pre_Key, Post_Key, Alias)
        for idx, (pre_key, post_key, alias_val) in enumerate(sublot_mappings):
            if not pre_key or not post_key:
                log_callback(f"Warning: Skipping empty mapping row {idx+1}")
                continue
                
            log_callback(f"  > Mapping {idx+1}: '{pre_key}' <-> '{post_key}'. Alias: '{alias_val}'")
            
            # Check for ungrouped rows
            ungrouped_mask = raw_df['__group_id__'].isna()
            
            # 1. Identify distinct sets of rows for Pre and Post
            mask_pre = raw_df[sublot_col].str.contains(pre_key, case=False, na=False) & ungrouped_mask
            mask_post = raw_df[sublot_col].str.contains(post_key, case=False, na=False) & ungrouped_mask
            
            df_pre_subset = raw_df.loc[mask_pre, [source_slot_col]]
            df_post_subset = raw_df.loc[mask_post, [source_slot_col]]
            
            if df_pre_subset.empty or df_post_subset.empty:
                log_callback(f"    Skipping: One of the sets is empty.")
                continue

            # 2. Strict Merge Pairing on Slot
            merge_pairs = pd.merge(
                df_pre_subset.reset_index(), 
                df_post_subset.reset_index(), 
                on=source_slot_col, 
                how='inner', 
                suffixes=('_pre', '_post')
            )
            
            # Remove duplicates to ensure 1-to-1 pairing per slot
            merge_pairs = merge_pairs.drop_duplicates(subset=[source_slot_col])
            # Prevent self-pairing
            merge_pairs = merge_pairs[merge_pairs['index_pre'] != merge_pairs['index_post']]
            
            if merge_pairs.empty:
                log_callback(f"    No matching slots found for this mapping.")
                continue
                
            # 3. Assign Group IDs and Alias
            local_pairs = 0
            for _, row in merge_pairs.iterrows():
                idx_pre = row['index_pre']
                idx_post = row['index_post']
                
                # Double check availability
                if pd.isna(raw_df.at[idx_pre, '__group_id__']) and pd.isna(raw_df.at[idx_post, '__group_id__']):
                    group_counter += 1
                    raw_df.at[idx_pre, '__group_id__'] = f"G{group_counter}"
                    raw_df.at[idx_post, '__group_id__'] = f"G{group_counter}"
                    
                    # Store the user defined alias if provided
                    if alias_val:
                        raw_df.at[idx_pre, '__x_alias__'] = alias_val
                        raw_df.at[idx_post, '__x_alias__'] = alias_val
                        
                    local_pairs += 1
            
            log_callback(f"    Successfully paired {local_pairs} slots.")
        
        # --- Separation for Advanced Mode ---
        # Sort by GroupID then Time
        raw_df = raw_df.sort_values(by=['__group_id__', '__dt_obj__'])
        raw_df['__seq__'] = raw_df.groupby('__group_id__').cumcount()
        
        df_pre = raw_df[raw_df['__seq__'] == 0].copy()
        df_post = raw_df[raw_df['__seq__'] == 1].copy()

        pre_prefix = f"{pre_label_user}_"
        post_prefix = f"{post_label_user}_"
        
        df_pre_renamed = df_pre.rename(columns={col: f'{pre_prefix}{col}' for col in original_cols})
        df_post_renamed = df_post.rename(columns={col: f'{post_prefix}{col}' for col in original_cols})

        log_callback(f"Pairing Result: {len(df_pre)} {pre_label_user} vs {len(df_post)} {post_label_user}.")

        # --- Merge ---
        # We also bring in __x_alias__ from pre (it matches post anyway)
        merge_cols = ['__group_id__'] + [f'{post_prefix}{col}' for col in original_cols]
        
        merged_df = pd.merge(
            df_pre_renamed, 
            df_post_renamed[merge_cols], 
            on='__group_id__', 
            how='left'
        )
        
        final_order = [f'{pre_prefix}{col}' for col in original_cols] + [f'{post_prefix}{col}' for col in original_cols]
        final_order_existing = [col for col in final_order if col in merged_df.columns]
        final_df = merged_df[final_order_existing].copy()
        
        # --- Alias Application (Override Sublot Column for X-Axis) ---
        if '__x_alias__' in merged_df.columns:
            # Where alias is not null, override the Sublot column
            target_sublot_col = f'{pre_prefix}{sublot_col}'
            if target_sublot_col in final_df.columns:
                mask_alias = merged_df['__x_alias__'].notna()
                # We use values from merged_df to update final_df
                if mask_alias.any():
                    log_callback("Applying custom X-Axis aliases...")
                    # Ensure index alignment
                    final_df.loc[mask_alias, target_sublot_col] = merged_df.loc[mask_alias, '__x_alias__']

    else:
        # ==========================================
        # MODE 1: AUTO (Universal Pairing)
        # ==========================================
        log_callback("Running Auto Logic: Universal Bridge Pairing...")
        temp_df = raw_df.copy()
        
        temp_df['__key_phys__'] = temp_df[sublot_col] + "_S" + temp_df[source_slot_col] 
        temp_df['__key_logi__'] = temp_df[wafer_col] + "_S" + temp_df[source_slot_col]  

        group_counter = 0

        # PRIORITY 1: Physical Bridge
        phys_groups = temp_df.groupby('__key_phys__')
        for _, group in phys_groups:
            if len(group) >= 2:
                indices = group.index
                if pd.isna(temp_df.loc[indices[0], '__group_id__']):
                    group_counter += 1
                    raw_df.loc[[indices[0], indices[-1]], '__group_id__'] = f"G{group_counter}"
                    temp_df.loc[[indices[0], indices[-1]], '__group_id__'] = f"G{group_counter}"

        # PRIORITY 2: Logical Bridge
        unpaired_mask = raw_df['__group_id__'].isna()
        temp_df.loc[~unpaired_mask, '__group_id__'] = raw_df.loc[~unpaired_mask, '__group_id__']
        
        logi_groups = temp_df[unpaired_mask].groupby('__key_logi__')
        for _, group in logi_groups:
            if len(group) >= 2:
                indices = group.index
                group_counter += 1
                raw_df.loc[[indices[0], indices[-1]], '__group_id__'] = f"G{group_counter}"

        # Label Orphans
        orphan_mask = raw_df['__group_id__'].isna()
        if orphan_mask.any():
            raw_df.loc[orphan_mask, '__group_id__'] = "O" + raw_df.loc[orphan_mask].index.astype(str)

        # --- Separation ---
        # Sort by GroupID then Time
        raw_df = raw_df.sort_values(by=['__group_id__', '__dt_obj__'])
        raw_df['__seq__'] = raw_df.groupby('__group_id__').cumcount()
        
        df_pre = raw_df[raw_df['__seq__'] == 0].copy()
        df_post = raw_df[raw_df['__seq__'] == 1].copy()

        pre_prefix = f"{pre_label_user}_"
        post_prefix = f"{post_label_user}_"
        
        df_pre_renamed = df_pre.rename(columns={col: f'{pre_prefix}{col}' for col in original_cols})
        df_post_renamed = df_post.rename(columns={col: f'{post_prefix}{col}' for col in original_cols})

        log_callback(f"Pairing Result: {len(df_pre)} {pre_label_user} vs {len(df_post)} {post_label_user}.")

        # --- Merge ---
        # We also bring in __x_alias__ from pre (it matches post anyway)
        merge_cols = ['__group_id__'] + [f'{post_prefix}{col}' for col in original_cols]
        
        merged_df = pd.merge(
            df_pre_renamed, 
            df_post_renamed[merge_cols], 
            on='__group_id__', 
            how='left'
        )
        
        final_order = [f'{pre_prefix}{col}' for col in original_cols] + [f'{post_prefix}{col}' for col in original_cols]
        final_order_existing = [col for col in final_order if col in merged_df.columns]
        final_df = merged_df[final_order_existing].copy()
        
        # --- Alias Application (Override Sublot Column for X-Axis) ---
        if '__x_alias__' in merged_df.columns:
            # Where alias is not null, override the Sublot column
            target_sublot_col = f'{pre_prefix}{sublot_col}'
            if target_sublot_col in final_df.columns:
                mask_alias = merged_df['__x_alias__'].notna()
                # We use values from merged_df to update final_df
                if mask_alias.any():
                    log_callback("Applying custom X-Axis aliases...")
                    # Ensure index alignment
                    final_df.loc[mask_alias, target_sublot_col] = merged_df.loc[mask_alias, '__x_alias__']

    # --- Data Column Detection ---
    potential_data_cols = [c for c in original_cols if c not in required_cols and c != date_col]
    actual_data_cols = []
    
    pre_prefix = f"{pre_label_user}_"
    post_prefix = f"{post_label_user}_"

    for base_col in potential_data_cols:
        col_p = f'{pre_prefix}{base_col}'
        col_f = f'{post_prefix}{base_col}'
        
        if col_p in final_df.columns:
            final_df[col_p] = pd.to_numeric(final_df[col_p], errors='coerce')
            if final_df[col_p].notna().sum() > 0:
                actual_data_cols.append(base_col)
        
        if mode != 'DP Only' and col_f in final_df.columns:
            final_df[col_f] = pd.to_numeric(final_df[col_f], errors='coerce')

    # --- Delta Calculation (Skipped for Mode 3) ---
    if mode != 'DP Only' and not df_post.empty:
        log_callback(f"Calculating Delta ({len(actual_data_cols)} params)...")
        for base_col in actual_data_cols:
            col_p = f'{pre_prefix}{base_col}'
            col_f = f'{post_prefix}{base_col}'
            del_col = f'DEL_{base_col}'
            
            if col_p in final_df.columns and col_f in final_df.columns:
                if "Thickness" in base_col:
                    final_df[del_col] = final_df[col_p] - final_df[col_f]
                else:
                    final_df[del_col] = final_df[col_f] - final_df[col_p]

    # ==========================================
    # STATISTICS & CLEANING (Mode 1 Specific)
    # ==========================================
    if mode == 'Auto':
        log_callback("Performing Mode 1 Specific Cleaning & Statistics...")
        
        # Define Columns for checking
        post_dev_col = f'{post_prefix}{device_col}'
        pre_dev_col = f'{pre_prefix}{device_col}'
        
        # 1. Identify Orphans (Rows where Post-side Device is missing/NaN)
        # Note: 'how=left' merge ensures Pre exists, but Post might be NaN
        is_orphan = final_df[post_dev_col].isna()
        
        # Split into Orphans vs Paired
        df_orphans = final_df[is_orphan]
        df_valid = final_df[~is_orphan]
        
        # 2. Categorize Orphans
        # Type 1: Orphan DP (Pre Device is FPMS004 or FPMS007) -> DELETE
        target_devs = ["FPMS004", "FPMS007"]
        # Ensure string comparison
        if pre_dev_col in df_orphans.columns:
            orphan_devs = df_orphans[pre_dev_col].astype(str)
            mask_orphan_dp = orphan_devs.isin(target_devs)
            count_orphan_dp = mask_orphan_dp.sum()
            
            # Type 2: Orphan FP (Pre Device is NOT FPMS004/007) -> DELETE
            # (These are likely FPs that ended up in the DP slot because they had no pair)
            mask_orphan_fp = ~mask_orphan_dp
            count_orphan_fp = mask_orphan_fp.sum()
        else:
            count_orphan_dp = 0
            count_orphan_fp = len(df_orphans)
        
        # 3. Apply Deletion (Keep only valid pairs)
        final_df = df_valid.copy()
        
        # 4. Count Delta / ERO / Partial
        # Count rows with valid Delta (e.g., using MaxE or first data column)
        # "delta count (rows where delta_MaxE has value)"
        del_maxe_col = 'DEL_MaxE'
        if del_maxe_col in final_df.columns:
            count_delta_ok = final_df[del_maxe_col].notna().sum()
        else:
            # Fallback: check first available DEL column or just row count
            delta_cols = [c for c in final_df.columns if c.startswith('DEL_')]
            if delta_cols:
                count_delta_ok = final_df[delta_cols[0]].notna().sum()
            else:
                count_delta_ok = len(final_df) # Should technically be all valid pairs
                
        # Count "Partial/ERO"
        # "Rows with some missing data, usually starts with ERO147"
        # We define this as rows in the FINAL set (non-deleted) that have missing Delta values
        if actual_data_cols:
            delta_cols_all = [f'DEL_{c}' for c in actual_data_cols if f'DEL_{c}' in final_df.columns]
            if delta_cols_all:
                # Rows where ANY delta column is NaN
                has_nan = final_df[delta_cols_all].isna().any(axis=1)
                count_partial = has_nan.sum()
            else:
                count_partial = 0
        else:
            count_partial = 0

        # Log Report
        stats_msg = (
            f"\n--- [Mode 1 Statistics] ---\n"
            f"1. Total Original Rows: {total_raw_rows}\n"
            f"2. Valid Delta Count (Pairs): {count_delta_ok}\n"
            f"3. Deleted Orphan DP (FPMS004/007): {count_orphan_dp}\n"
            f"4. Deleted Orphan FP (Others): {count_orphan_fp}\n"
            f"5. Partial/ERO Data (Kept): {count_partial}\n"
            f"---------------------------"
        )
        # ONLY log this to file (as well as screen)
        # To make this work without erroring if 'to_file' argument is not supported by standard print, 
        # we rely on the log_callback implementation in DataReportFunction
        try:
            log_callback(stats_msg, to_file=True)
        except TypeError:
             # Fallback for old loggers
             log_callback(stats_msg)
    
    return final_df, original_cols, actual_data_cols, pre_label_user, post_label_user

# -----------------------------------------------------------------------------
# DataReportFunction (Adapted for FPAnalysisApp)
# -----------------------------------------------------------------------------
class DataReportFunction:
    """Automated Data Report Tool (Universal Pro) Integration"""
    def __init__(self, app):
        self.app = app
        self.frame = None
        self.filepath = None
        self.processed_df = None
        self.original_cols = None
        self.data_cols = None
        self.output_dir = None
        self.input_filename_no_ext = None
        self.mapping_rows = [] 
        self.current_log_path = None 
        
        self.cur_pre_label = "DP"
        self.cur_post_label = "FP"

    def show(self):
        if self.frame: self.frame.destroy()
        self.frame = ttk.Frame(self.app.right_frame)
        self.frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Title
        title_label = ttk.Label(self.frame, text="Automated Data Report Tool (Universal Pro)", font=("", 14, "bold"))
        title_label.pack(pady=(0, 15))

        # --- MODE SELECTION ---
        mode_frame = ttk.LabelFrame(self.frame, text=" 1. Mode Selection ", padding=10)
        mode_frame.pack(fill=tk.X, pady=(0, 10))

        self.mode_var = tk.StringVar(value="Auto")
        
        rb_auto = ttk.Radiobutton(mode_frame, text="Mode 1: Auto (Standard DP/FP)", variable=self.mode_var, value="Auto", command=self.toggle_mode_ui)
        rb_auto.grid(row=0, column=0, sticky='w', padx=10)
        
        rb_adv = ttk.Radiobutton(mode_frame, text="Mode 2: Advanced (Manual Mapping)", variable=self.mode_var, value="Advanced (Cross-Sublot)", command=self.toggle_mode_ui)
        rb_adv.grid(row=0, column=1, sticky='w', padx=10)

        rb_dp = ttk.Radiobutton(mode_frame, text="Mode 3: DP Only", variable=self.mode_var, value="DP Only", command=self.toggle_mode_ui)
        rb_dp.grid(row=0, column=2, sticky='w', padx=10)

        # --- ADVANCED OPTIONS CONTAINER ---
        self.adv_frame = ttk.Frame(mode_frame)
        self.adv_frame.grid(row=1, column=0, columnspan=3, sticky='nsew', padx=10, pady=5)
        
        # Labels
        lbl_frame = ttk.Frame(self.adv_frame)
        lbl_frame.pack(fill=tk.X, pady=5)
        ttk.Label(lbl_frame, text="Chart Pre-Label:").pack(side=tk.LEFT, padx=5)
        self.pre_label_cb = ttk.Combobox(lbl_frame, values=STAGE_OPTIONS, width=15)
        self.pre_label_cb.set("PRE_2000")
        self.pre_label_cb.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(lbl_frame, text="Chart Post-Label:").pack(side=tk.LEFT, padx=5)
        self.post_label_cb = ttk.Combobox(lbl_frame, values=STAGE_OPTIONS, width=15)
        self.post_label_cb.set("POST_2000")
        self.post_label_cb.pack(side=tk.LEFT, padx=5)

        # Buttons
        btn_frame = ttk.Frame(self.adv_frame)
        btn_frame.pack(fill=tk.X, pady=2)
        ttk.Button(btn_frame, text="[+] Add Mapping Row", command=self.add_mapping_row).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="[-] Remove Last Row", command=self.remove_mapping_row).pack(side=tk.LEFT, padx=5)

        # Headers
        header_frame = ttk.Frame(self.adv_frame)
        header_frame.pack(fill=tk.X, pady=(5, 0))
        ttk.Label(header_frame, text="Pre-Sublot (Keyword)", width=25, font=("", 9, "bold")).pack(side=tk.LEFT, padx=5)
        ttk.Label(header_frame, text="Post-Sublot (Keyword)", width=25, font=("", 9, "bold")).pack(side=tk.LEFT, padx=5)
        ttk.Label(header_frame, text="Custom X-Axis Name", width=25, font=("", 9, "bold")).pack(side=tk.LEFT, padx=5)

        # Scrolled Area Implementation using Canvas
        self.canvas_container = ttk.Frame(self.adv_frame)
        self.canvas_container.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.mapping_canvas = tk.Canvas(self.canvas_container, height=120)
        self.scrollbar = ttk.Scrollbar(self.canvas_container, orient="vertical", command=self.mapping_canvas.yview)
        self.scrollable_frame = ttk.Frame(self.mapping_canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.mapping_canvas.configure(
                scrollregion=self.mapping_canvas.bbox("all")
            )
        )

        self.mapping_canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.mapping_canvas.configure(yscrollcommand=self.scrollbar.set)

        self.mapping_canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")
        
        self.add_mapping_row()
        self.toggle_mode_ui() # Initial hide if Auto

        # --- CONFIGURATION ---
        settings_frame = ttk.LabelFrame(self.frame, text=" 2. Chart Configuration ", padding=10)
        settings_frame.pack(fill=tk.X, pady=(0, 10))
        
        grid_f = ttk.Frame(settings_frame)
        grid_f.pack(fill=tk.X)

        ttk.Label(grid_f, text="Select Chart X-Axis:").grid(row=0, column=0, sticky='w', padx=5, pady=5)
        self.xaxis_choice = tk.StringVar(value="Sublot")
        self.xaxis_menu = ttk.Combobox(grid_f, textvariable=self.xaxis_choice, values=["Sublot", "Wafer ID"], state="readonly", width=25)
        self.xaxis_menu.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(grid_f, text="Output Image Quality:").grid(row=1, column=0, sticky='w', padx=5, pady=5)
        self.quality_choice = tk.StringVar(value="High Definition (300 DPI)")
        self.quality_menu = ttk.Combobox(grid_f, textvariable=self.quality_choice, 
                                         values=["Standard (120 DPI)", "High Definition (300 DPI)"], 
                                         state="readonly", width=25)
        self.quality_menu.grid(row=1, column=1, padx=5, pady=5)
        
        # Checkbox for X-Axis Title
        self.show_xlabel_var = tk.BooleanVar(value=True)
        self.chk_xlabel = ttk.Checkbutton(grid_f, text="Show X-Axis Title (e.g. 'Sublot')", variable=self.show_xlabel_var)
        self.chk_xlabel.grid(row=2, column=0, columnspan=2, sticky='w', padx=5, pady=5)

        # --- ACTIONS ---
        action_frame = ttk.Frame(self.frame)
        action_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.process_button = ttk.Button(action_frame, text="Select File & Run", command=self.start_single_report_thread)
        self.process_button.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=5)
        
        self.generate_all_button = ttk.Button(action_frame, text="Batch Generate (Sublot & Wafer)", command=self.start_generate_all_thread)
        self.generate_all_button.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=5)
        
        # Log Area
        log_labelframe = ttk.LabelFrame(self.frame, text=" Run Log ")
        log_labelframe.pack(fill=tk.BOTH, expand=True)

        self.status_text = scrolledtext.ScrolledText(log_labelframe, wrap=tk.WORD, height=10, relief="flat", font=("Consolas", 9))
        self.status_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.log(f"System Ready. Mode: Universal Pairing Logic. Font: Times New Roman.")

    def add_mapping_row(self):
        row_frame = ttk.Frame(self.scrollable_frame)
        row_frame.pack(fill=tk.X, pady=2)
        
        ent_pre = ttk.Entry(row_frame, width=25)
        ent_pre.pack(side=tk.LEFT, padx=5)
        
        ent_post = ttk.Entry(row_frame, width=25)
        ent_post.pack(side=tk.LEFT, padx=5)
        
        ent_alias = ttk.Entry(row_frame, width=25)
        ent_alias.pack(side=tk.LEFT, padx=5)
        
        self.mapping_rows.append((row_frame, ent_pre, ent_post, ent_alias))

    def remove_mapping_row(self):
        if len(self.mapping_rows) > 1: 
            row_frame, _, _, _ = self.mapping_rows.pop()
            row_frame.destroy()

    def toggle_mode_ui(self):
        if self.mode_var.get() == "Auto" or self.mode_var.get() == "DP Only":
            self.adv_frame.grid_remove() # Hide completely
        else:
            self.adv_frame.grid() # Show

    def get_dpi(self):
        choice = self.quality_choice.get()
        return 300 if "300" in choice else 120

    def get_config(self):
        mappings = []
        mode = self.mode_var.get()
        
        if mode == "Advanced (Cross-Sublot)":
            for _, ent_pre, ent_post, ent_alias in self.mapping_rows:
                p = ent_pre.get().strip()
                f = ent_post.get().strip()
                alias = ent_alias.get().strip()
                if p and f:
                    mappings.append((p, f, alias))
        
        if mode == "Auto":
            p_label = "DP"
            f_label = "FP"
        elif mode == "DP Only":
            p_label = "DP" 
            f_label = "NONE"
        else:
            p_label = self.pre_label_cb.get()
            f_label = self.post_label_cb.get()

        return {
            'mode': mode,
            'sublot_mappings': mappings,
            'pre_label': p_label,
            'post_label': f_label
        }

    def log(self, message, to_file=False):
        if self.frame and self.frame.winfo_exists():
            self.app.root.after(0, self._log_thread_safe, message)
        
        if to_file and self.current_log_path:
            try:
                with open(self.current_log_path, "a", encoding="utf-8") as f:
                    f.write(message.strip() + "\n")
            except Exception:
                pass 

    def _log_thread_safe(self, message):
        self.status_text.insert(tk.END, message + "\n")
        self.status_text.see(tk.END)

    def set_buttons_state(self, state):
        self.process_button.config(state=state)
        self.generate_all_button.config(state=state)

    def set_buttons_state_wrapper(self, enable):
        state = tk.NORMAL if enable else tk.DISABLED
        self.set_buttons_state(state)

    # --- Threading Workers ---
    def start_single_report_thread(self):
        filepath = filedialog.askopenfilename(title="Select Measurement Data", filetypes=[("CSV files", "*.csv")])
        if not filepath: return
        self.filepath = filepath
        x_label = self.xaxis_choice.get()
        x_col = 'DP_Sublot' if x_label == "Sublot" else 'DP_Wafer ID'
        config = self.get_config()
        self.app.start_thread(lambda: self.run_process(x_col, x_label, config), self.set_buttons_state_wrapper)

    def start_generate_all_thread(self):
        filepath = filedialog.askopenfilename(title="Select Measurement Data", filetypes=[("CSV files", "*.csv")])
        if not filepath: return
        self.filepath = filepath
        self.set_buttons_state(tk.DISABLED)
        config = self.get_config()
        self.app.start_thread(lambda: self.run_process_batch(config), self.set_buttons_state_wrapper)

    def run_process(self, x_col, x_label, config):
        try:
            # Setup log file
            self.output_dir = os.path.dirname(self.filepath)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            self.current_log_path = os.path.join(self.output_dir, f"log_{timestamp}.log")
            self.log(f"Log file initialized: {self.current_log_path}")

            self.processed_df, _, self.data_cols, self.cur_pre_label, self.cur_post_label = process_and_clean_data_final(self.filepath, self.log, config)
            
            self.input_filename_no_ext = os.path.splitext(os.path.basename(self.filepath))[0]

            sort_columns = [f"{self.cur_pre_label}_Date", f"{self.cur_pre_label}_Sublot", f"{self.cur_pre_label}_Wafer ID"]
            valid_sort_cols = [c for c in sort_columns if c in self.processed_df.columns]
            if valid_sort_cols:
                self.processed_df = self.processed_df.sort_values(by=valid_sort_cols, ascending=True)
            
            out_csv = os.path.join(self.output_dir, f"processed_{self.input_filename_no_ext}.csv")
            self.processed_df.to_csv(out_csv, index=False, encoding='utf_8_sig')
            self.log(f"CSV saved to: {out_csv}")
            
            self.log("\nSTEP 2: Generating HD Charts...")
            
            # Use dynamic labels based on configuration (DP/FP or PRE_2000/POST_2000 etc.)
            # The column in processed_df will be named like "PRE_2000_Sublot" or "DP_Sublot"
            suffix = 'Sublot' if x_label == "Sublot" else 'Wafer ID'
            x_col_dynamic = f"{self.cur_pre_label}_{suffix}"
            
            self.run_report_generation(x_col_dynamic, x_label, self.show_xlabel_var.get())
            self.app.root.after(0, lambda: messagebox.showinfo("Success", f"Report generation complete!"))
        except Exception as e:
            self.log(f"Error: {e}")
            self.app.root.after(0, lambda: messagebox.showerror("Error", str(e)))
        finally:
            self.current_log_path = None 

    def run_process_batch(self, config):
        try:
            # Setup log file
            self.output_dir = os.path.dirname(self.filepath)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            self.current_log_path = os.path.join(self.output_dir, f"log_{timestamp}.log")
            self.log(f"Log file initialized: {self.current_log_path}")

            self.processed_df, _, self.data_cols, self.cur_pre_label, self.cur_post_label = process_and_clean_data_final(self.filepath, self.log, config)
            self.output_dir = os.path.dirname(self.filepath)
            self.input_filename_no_ext = os.path.splitext(os.path.basename(self.filepath))[0]

            sort_columns = [f"{self.cur_pre_label}_Date", f"{self.cur_pre_label}_Sublot", f"{self.cur_pre_label}_Wafer ID"]
            valid_sort_cols = [c for c in sort_columns if c in self.processed_df.columns]
            if valid_sort_cols:
                self.processed_df = self.processed_df.sort_values(by=valid_sort_cols, ascending=True)

            out_csv = os.path.join(self.output_dir, f"processed_{self.input_filename_no_ext}.csv")
            self.processed_df.to_csv(out_csv, index=False, encoding='utf_8_sig')
            self.log(f"CSV saved to: {out_csv}")

            self.log("\nSTEP 2: Generating Sublot Reports...")
            x_col_sublot = f"{self.cur_pre_label}_Sublot"
            self.run_report_generation(x_col_sublot, 'Sublot', self.show_xlabel_var.get())
            
            if x_col_sublot in self.processed_df.columns:
                sublot_count = self.processed_df[x_col_sublot].nunique()
            else:
                sublot_count = 0
                
            if sublot_count <= 5:
                self.log("\nSTEP 3: Generating Wafer ID Reports...")
                x_col_wafer = f"{self.cur_pre_label}_Wafer ID"
                self.run_report_generation(x_col_wafer, 'Wafer ID', self.show_xlabel_var.get())
            else:
                self.log(f"Skipped Wafer ID reports to avoid clutter.")

            self.app.root.after(0, lambda: messagebox.showinfo("Complete", "Batch processing successful."))
        except Exception as e:
            self.log(f"Error: {e}")
            self.app.root.after(0, lambda: messagebox.showerror("Error", str(e)))
        finally:
            self.current_log_path = None

    def run_report_generation(self, x_col, x_label, show_xlabel):
        base_folder = os.path.join(self.output_dir, f"plots_{self.input_filename_no_ext}")
        plot_path = os.path.join(base_folder, x_label); os.makedirs(plot_path, exist_ok=True)
        target_dpi = self.get_dpi()
        sublot_col_name = f"{self.cur_pre_label}_Sublot"
        sublot_count = self.processed_df[sublot_col_name].nunique() if sublot_col_name in self.processed_df.columns else 0
        skip_line = (x_label == "Sublot" and sublot_count <= 2)
        
        # Override Box Plot X-Axis if needed
        if x_label == "Wafer ID":
            box_x_override = sublot_col_name
        else:
            box_x_override = x_col

        for i, col in enumerate(self.data_cols):
            self.log(f"  [{i+1}/{len(self.data_cols)}] Plotting: {col}")
            if not skip_line: self._plot_line(col, x_col, plot_path, x_label, target_dpi, show_xlabel)
            self._plot_box(col, box_x_override, plot_path, x_label, target_dpi, show_xlabel)
        if not skip_line: self._plot_collection('line', self.data_cols, x_col, plot_path, x_label, target_dpi)
        self._plot_collection('box', self.data_cols, box_x_override, plot_path, x_label, target_dpi)

    # --- Broken Axis Helper Logic ---
    def _detect_broken_axis(self, data_series, threshold_ratio=0.35, min_gap_abs=40):
        if data_series.empty or data_series.nunique() <= 1: return None
        vals = data_series.dropna().values
        if len(vals) == 0: return None
        data_range = np.max(vals) - np.min(vals)
        if data_range == 0: return None
        sorted_vals = np.sort(np.unique(vals))
        diffs = np.diff(sorted_vals)
        if len(diffs) == 0: return None
        max_gap_idx = np.argmax(diffs)
        max_gap = diffs[max_gap_idx]
        if max_gap > (threshold_ratio * data_range) and max_gap > min_gap_abs:
            return (sorted_vals[max_gap_idx], sorted_vals[max_gap_idx + 1])
        return None

    def _setup_broken_axes(self, fig, gs_slot, gap_info, data_min, data_max):
        inner_gs = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=gs_slot, hspace=0.1)
        ax_top, ax_bot = fig.add_subplot(inner_gs[0]), fig.add_subplot(inner_gs[1])
        padding_top = (data_max - gap_info[1]) * 0.15 if data_max > gap_info[1] else 1
        padding_bot = (gap_info[0] - data_min) * 0.15 if gap_info[0] > data_min else 1
        ax_top.set_ylim(gap_info[1] - (padding_top * 0.2), data_max + padding_top)
        ax_bot.set_ylim(data_min - padding_bot, gap_info[0] + (padding_bot * 0.2))
        ax_top.spines['bottom'].set_visible(False); ax_bot.spines['top'].set_visible(False)
        ax_top.xaxis.tick_top(); ax_top.tick_params(labeltop=False); ax_bot.xaxis.tick_bottom()
        d = .012 
        kwargs = dict(transform=ax_top.transAxes, color='k', clip_on=False, lw=1)
        ax_top.plot((-d, +d), (-d, +d), **kwargs); ax_top.plot((1 - d, 1 + d), (-d, +d), **kwargs)  
        kwargs.update(transform=ax_bot.transAxes) 
        ax_bot.plot((-d, +d), (1 - d, 1 + d), **kwargs); ax_bot.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs) 
        return ax_top, ax_bot

    def _apply_slanted_xticks(self, ax):
        labels = ax.get_xticklabels()
        num_labels = len(labels)
        
        # 基础轴线设置保持一致
        ax.xaxis.set_ticks_position('bottom')
        ax.spines['bottom'].set_position(('outward', 6))
        
        if num_labels <= 10:
            # 标签数量 <= 10：水平显示 (0度)
            ax.tick_params(axis='x', labelrotation=0, pad=5, direction='out', length=4)
            for label in labels:
                label.set_ha('center')   # 水平居中
                label.set_va('top')      # 靠顶部对齐 (悬挂在轴线下)
                label.set_rotation_mode('anchor')
        else:
            # 标签数量 > 10：垂直显示 (90度)
            ax.tick_params(axis='x', labelrotation=90, pad=5, direction='out', length=4)
            for label in labels:
                label.set_ha('right')    # 将字符串的末尾（右侧）锚定到刻度线
                label.set_va('center')   # 在垂直方向上居中对齐锚点
                label.set_rotation_mode('anchor')

    def _plot_line(self, base_col, x_col, folder, xlabel, dpi, show_xlabel):
        dp, fp = f'{self.cur_pre_label}_{base_col}', f'{self.cur_post_label}_{base_col}'
        d_list = []
        if dp in self.processed_df.columns:
            d = self.processed_df[[x_col, dp]].dropna().rename(columns={dp: 'Val'}); d['Type']=self.cur_pre_label; d_list.append(d)
        if fp in self.processed_df.columns:
            d = self.processed_df[[x_col, fp]].dropna().rename(columns={fp: 'Val'}); d['Type']=self.cur_post_label; d_list.append(d)
        if not d_list: return
        combined = pd.concat(d_list); combined[x_col] = combined[x_col].astype(str)
        gap = self._detect_broken_axis(combined['Val'])
        if gap:
            fig = plt.figure(figsize=(14, 8)); gs = gridspec.GridSpec(1, 1)[0]
            ax_t, ax_b = self._setup_broken_axes(fig, gs, gap, combined['Val'].min(), combined['Val'].max())
            for ax in [ax_t, ax_b]:
                sns.lineplot(data=combined, x=x_col, y='Val', hue='Type', style='Type', markers=True, ax=ax, errorbar='sd')
                ax.legend().remove()
            ax_t.set_title(f"{base_col} (Broken Axis)", fontsize=16)
            if show_xlabel: ax_b.set_xlabel(xlabel)
            else: ax_b.set_xlabel("")
            fig.text(0.02, 0.5, base_col, va='center', rotation='vertical', fontsize=12)
            if "Sublot" in x_col:
                self._apply_slanted_xticks(ax_b)
            ax_t.legend(loc='upper right', frameon=True)
        else:
            fig, ax = plt.subplots(figsize=(14, 7))
            sns.lineplot(data=combined, x=x_col, y='Val', hue='Type', style='Type', markers=True, ax=ax, errorbar='sd')
            ax.set_title(f"{base_col}", fontsize=16)
            if show_xlabel: ax.set_xlabel(xlabel)
            else: ax.set_xlabel("")
            ax.set_ylabel(base_col)
            if "Sublot" in x_col:
                self._apply_slanted_xticks(ax)
            fig.tight_layout()
        plt.savefig(os.path.join(folder, f"Line_{sanitize_filename(base_col)}.png"), dpi=dpi, bbox_inches='tight'); plt.close(fig)
        

    def _plot_box(self, base_col, x_col, folder, xlabel, dpi, show_xlabel):
        dp, fp, dl = f'{self.cur_pre_label}_{base_col}', f'{self.cur_post_label}_{base_col}', f'DEL_{base_col}'
        has_delta = (dl in self.processed_df.columns) and (self.processed_df[dl].notna().sum() > 0)
        
        if has_delta:
            fig = plt.figure(figsize=(15, 12)); gs = gridspec.GridSpec(2, 1, hspace=0.5, height_ratios=[1, 1])
        else:
            fig = plt.figure(figsize=(15, 7)); gs = gridspec.GridSpec(1, 1)

        sublot_col_name = f"{self.cur_pre_label}_Sublot"
        display_xlabel = "Sublot" if x_col == sublot_col_name else xlabel
        
        v_list = []
        if dp in self.processed_df.columns:
            d = self.processed_df[[x_col, dp]].dropna().rename(columns={dp: 'Val'}); d['Type']=self.cur_pre_label; v_list.append(d)
        if fp in self.processed_df.columns:
            d = self.processed_df[[x_col, fp]].dropna().rename(columns={fp: 'Val'}); d['Type']=self.cur_post_label; v_list.append(d)
        
        if v_list:
            comb = pd.concat(v_list); gap_v = self._detect_broken_axis(comb['Val'])
            if gap_v:
                ax1_t, ax1_b = self._setup_broken_axes(fig, gs[0], gap_v, comb['Val'].min(), comb['Val'].max())
                for ax in [ax1_t, ax1_b]: 
                    sns.boxplot(data=comb, x=x_col, y='Val', hue='Type', ax=ax)
                    ax.legend().remove()
                    ax.set_ylabel(base_col) 
                ax1_t.set_title(f"{base_col} (Broken Axis)", fontsize=14); ax1_t.legend(loc='upper right')
                if show_xlabel and not has_delta: ax1_b.set_xlabel(display_xlabel) 
                if "Sublot" in x_col:
                    self._apply_slanted_xticks(ax1_b)
            else:
                ax1 = fig.add_subplot(gs[0]); sns.boxplot(data=comb, x=x_col, y='Val', hue='Type', ax=ax1)
                ax1.set_title(f"{base_col}", fontsize=14)
                ax1.set_ylabel(base_col) 
                if show_xlabel and not has_delta: ax1.set_xlabel(display_xlabel)
                else: ax1.set_xlabel("")
                if "Sublot" in x_col:
                    self._apply_slanted_xticks(ax1)
                
        if has_delta:
            delta_data = self.processed_df[[x_col, dl]].dropna(); gap_d = self._detect_broken_axis(delta_data[dl])
            calc_tag = f"({self.cur_pre_label}-{self.cur_post_label})" if "Thickness" in base_col else f"({self.cur_post_label}-{self.cur_pre_label})"
            title_text = f"{dl} {calc_tag}" 
            
            if gap_d:
                ax2_t, ax2_b = self._setup_broken_axes(fig, gs[1], gap_d, delta_data[dl].min(), delta_data[dl].max())
                for ax in [ax2_t, ax2_b]: sns.boxplot(data=delta_data, x=x_col, y=dl, ax=ax, color="#5dbb63")
                ax2_t.set_title(f"{title_text} (Broken Axis)", fontsize=14)
                if show_xlabel: ax2_b.set_xlabel(display_xlabel)
                else: ax2_b.set_xlabel("")
                if "Sublot" in x_col:
                    self._apply_slanted_xticks(ax2_b)
            else:
                ax2 = fig.add_subplot(gs[1]); sns.boxplot(data=delta_data, x=x_col, y=dl, ax=ax2, color="#5dbb63")
                ax2.set_title(f"{title_text}", fontsize=14)
                if show_xlabel: ax2.set_xlabel(display_xlabel)
                else: ax2.set_xlabel("")
                if "Sublot" in x_col:
                    self._apply_slanted_xticks(ax2)
        plt.savefig(os.path.join(folder, f"Box_{sanitize_filename(base_col)}.png"), dpi=dpi, bbox_inches='tight'); plt.close(fig)

    def _plot_collection(self, p_type, params, x_col, folder, xlabel, dpi):
        n = len(params); cols = 4; rows = (n - 1) // cols + 1
        fig, axes = plt.subplots(rows, cols, figsize=(24, rows * 5)); axes = axes.flatten()
        for i, col in enumerate(params):
            ax = axes[i]; dp, fp, dl = f'{self.cur_pre_label}_{col}', f'{self.cur_post_label}_{col}', f'DEL_{col}'
            if p_type == 'line':
                l_list = []
                if dp in self.processed_df.columns: l_list.append(self.processed_df[[x_col, dp]].dropna().rename(columns={dp: 'V', x_col: 'X'}).assign(T=self.cur_pre_label))
                if fp in self.processed_df.columns: l_list.append(self.processed_df[[x_col, fp]].dropna().rename(columns={fp: 'V', x_col: 'X'}).assign(T=self.cur_post_label))
                if l_list:
                    comb = pd.concat(l_list); comb['X'] = comb['X'].astype(str)
                    sns.lineplot(data=comb, x='X', y='V', hue='T', ax=ax, legend=False, errorbar=None)
            else:
                if dl in self.processed_df.columns:
                    sns.boxplot(data=self.processed_df[[x_col, dl]].dropna().rename(columns={dl: 'V'}), x=x_col, y='V', ax=ax, color='lightgray')
            ax.set_title(col, fontsize=11); ax.set_xlabel(''); ax.set_ylabel('')
            if "Sublot" in x_col:
                self._apply_slanted_xticks(ax)
        for j in range(i + 1, len(axes)): fig.delaxes(axes[j])
        fig.suptitle(f"Collection: {p_type.upper()} Summary", fontsize=26); plt.tight_layout(rect=[0, 0, 1, 0.97])
        plt.savefig(os.path.join(folder, f"__Summary_{p_type}.png"), dpi=dpi, bbox_inches='tight'); plt.close(fig)

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
        
        # Optimization: Warm up the database connection (JVM start) in background
        self.warmup_db()

    def warmup_db(self):
        import time
        # ========================================================
        # 优化 1: 界面感知 - 在状态栏显示正在连接
        # ========================================================
        def update_status_label(msg):
            # 尝试找到当前界面的进度条标签并更新它
            # 我们遍历你所有的功能模块，看谁当前显示在界面上
            for func in [self.sublot_automation, self.automation, self.sublot_trace]:
                if hasattr(func, 'trace_progress_label') and func.trace_progress_label.winfo_exists():
                    func.trace_progress_label.config(text=msg)
                    break
        
        update_status_label("⏳ 系统正在后台初始化数据库组件，请稍候...")
        
        def _warmup():
            t_start = time.time()
            try:
                # ========================================================
                # 优化 2: 真正的数据库连接测试 (你可以去 DatabaseManager 里加内存限制)
                # ========================================================
                # Attempt a connection just to start JVM and load classes
                conn = DatabaseManager.get_db_connection(silent=True) 
                if conn:
                    conn.close()
                    t_end = time.time()
                    elapsed = t_end - t_start
                    
                    success_msg = f"Database warm-up successful (JVM started). 耗时: {elapsed:.2f} 秒"
                    print(f"\n[DB 探针] {success_msg}")
                    
                    # 使用 root.after 确保在主线程更新 GUI，避免跨线程报错
                    self.root.after(0, lambda: update_status_label(f"✅ 数据库就绪 (预热耗时: {elapsed:.2f}s)"))
                    
            except Exception as e:
                t_end = time.time()
                print(f"\n[DB 探针] Database warm-up failed after {t_end - t_start:.2f}s: {e}")
                self.root.after(0, lambda: update_status_label(f"❌ 数据库初始化失败: {e}"))
        
        # 启动后台守护线程
        threading.Thread(target=_warmup, daemon=True).start()

    def create_left_panel(self):
        ttk.Label(self.left_frame, text="功能选择", font=('Arial', 12, 'bold')).pack(pady=10, padx=10)
        # --- 修改: 添加新功能入口 'Data Report Tool'，重命名 'Product自动化处理' ---
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
        """Starts and manages a background thread for long-running tasks."""
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

    def update_progress(self, message: str, value: Optional[float] = None, feature_id: str = ''):
        """Thread-safely updates progress labels/bars in any feature."""
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
        """Handles application closing and ensures processes are terminated."""
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


if __name__ == "__main__":
    import time
    
    print("\n[启动探针] ====== 开始加载程序 ======")
    t0 = time.time()
    
    try:
        # 1. 记录 Tkinter 引擎初始化耗时
        t1 = time.time()
        root = tk.Tk()
        try:
            root.iconbitmap(DatabaseManager.get_resource_path("icon.ico"))
        except:
            pass
        t2 = time.time()
        print(f"[启动探针] 1. Tkinter 引擎加载与图标挂载耗时: {t2 - t1:.4f} 秒")
        
        # 2. 记录核心 App 实例化耗时 (最有可能卡顿的地方！)
        t3 = time.time()
        app = FPAnalysisApp(root)
        t4 = time.time()
        print(f"[启动探针] 2. 主程序 (FPAnalysisApp) 实例化耗时: {t4 - t3:.4f} 秒")
        
        print(f"[启动探针] ====== 准备就绪，总计启动耗时: {t4 - t0:.4f} 秒 ======\n")
        
        # 正式启动 GUI 事件循环
        root.mainloop()
        
    except Exception as e:
        import traceback
        print(f"[启动探针] 启动失败: {e}")
        traceback.print_exc()

