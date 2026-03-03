import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from tkcalendar import DateEntry
import csv
from datetime import datetime
from typing import List, Optional

from database import DatabaseManager

class SublotTraceFunction:
    """Sublot history tracing feature."""
    def __init__(self, app):
        self.app = app
        self.frame = None
    
    def show(self):
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
        self.eqp_listbox.selection_clear(0, tk.END)
        rd_tools = {'FPOL007', 'FPOL008', 'FPOL009', 'FPOL010'}
        for i, item in enumerate(self.eqp_listbox.get(0, tk.END)):
            if item in rd_tools:
                self.eqp_listbox.selection_set(i)
    
    def _select_non_rd_tools(self):
        self.eqp_listbox.selection_clear(0, tk.END)
        rd_tools = {'FPOL007', 'FPOL008', 'FPOL009', 'FPOL010'}
        for i, item in enumerate(self.eqp_listbox.get(0, tk.END)):
            if item not in rd_tools:
                self.eqp_listbox.selection_set(i)

    def _toggle_time_controls(self):
        state = tk.NORMAL if self.time_mode.get() == "custom" else tk.DISABLED
        self.start_date_entry.config(state=state)
        self.end_date_entry.config(state=state)

    def start_tracing_thread(self):
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

            self.app.update_progress("正在连接并查询数据库...", None, 'trace')
            
            results = self.run_database_query(**params)
            
            if self.frame.winfo_exists():
                self.app.root.after(0, self.display_results, results)

        self.app.start_thread(trace_logic, self._set_trace_controls_state)

    def run_database_query(self, selected_prod_id: Optional[str], selected_eqp: List[str] = None, sublot_ids: List[str] = None, time_mode: str = None, start_date: datetime.date = None, end_date: datetime.date = None) -> Optional[List[tuple]]:
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
                b.PROD_ID,
                b.SUBLOT_ID,
                MAX(CASE WHEN b.OPE_ID = '5110' THEN CAST(b.EQP_ID AS VARCHAR(20)) END) AS DPOL_EQP,
                MAX(CASE WHEN b.OPE_ID = '5110' THEN b.HIS_REGIST_DTTM END) AS DPOL_TIME,
                MAX(CASE WHEN b.OPE_ID = '5120' THEN CAST(b.EQP_ID AS VARCHAR(20)) END) AS DPGE_EQP,
                MAX(CASE WHEN b.OPE_ID = '5120' THEN b.HIS_REGIST_DTTM END) AS DPGE_TIME,
                MAX(CASE WHEN b.OPE_ID = '6040' THEN CAST(b.EQP_ID AS VARCHAR(20)) END) AS FPOL_EQP,
                MAX(CASE WHEN b.OPE_ID = '6040' THEN b.HIS_REGIST_DTTM END) AS FPOL_TIME,
                MAX(CASE WHEN b.OPE_ID = '7020' THEN CAST(b.EQP_ID AS VARCHAR(20)) END) AS FPMS_EQP,
                MAX(CASE WHEN b.OPE_ID = '7020' THEN b.HIS_REGIST_DTTM END) AS FPMS_TIME
            FROM DOPE_HIS AS b
            INNER JOIN TargetSublots AS t ON b.SUBLOT_ID = t.SUBLOT_ID
            WHERE b.OPE_ID IN ('5110', '5120', '6040', '7020')
            GROUP BY b.PROD_ID, b.SUBLOT_ID
            ORDER BY b.SUBLOT_ID
        """
        
        conn = None
        results = None
        try:
            conn = DatabaseManager.get_db_connection()
            if conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    results = cursor.fetchall()
        except Exception as e:
            self.app.update_progress(f"查询失败: {e}", None, 'trace')
            print(f"数据库查询错误: {e}")
        finally:
            if conn: conn.close()
        
        return results

    def display_results(self, results):
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