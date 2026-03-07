import os
import sys
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Checkbutton, scrolledtext
import csv
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
import re
import seaborn as sns
import warnings

from config import Config, STAGE_OPTIONS
from ui.auto_processing import process_and_clean_data_final, sanitize_filename

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