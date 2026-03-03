import tkinter as tk
import threading

# 导入配置和数据库（可选，用于预热）
from config import Config
from database import DatabaseManager

# 导入你拆分后的 UI 主框架
# 注意：你需要自己将原脚本的 FPAnalysisApp 类移动到 ui/main_window.py 中
from ui.main_window import FPAnalysisApp

def warmup_db():
    """后台预热数据库/JVM"""
    def _warmup():
        try:
            conn = DatabaseManager.get_db_connection(silent=True) 
            if conn:
                conn.close()
                print("Database warm-up successful (JVM started).")
        except Exception as e:
            print(f"Database warm-up failed (non-critical): {e}")
    
    threading.Thread(target=_warmup, daemon=True).start()

def main():
    root = tk.Tk()
    
    try:
        root.iconbitmap(DatabaseManager.get_resource_path("icon.ico"))
    except Exception:
        pass # 图标加载失败不影响运行

    # 初始化主应用
    app = FPAnalysisApp(root)
    
    # 启动后台预热
    warmup_db()
    
    # 启动主循环
    root.mainloop()

if __name__ == "__main__":
    main()