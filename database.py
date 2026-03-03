import os
import sys
import tkinter.messagebox as messagebox
import jaydebeapi
from config import Config

class DatabaseManager:
    """Manages database connections."""
    
    @staticmethod
    def get_resource_path(relative_path):
        """获取资源绝对路径（适配 PyInstaller 打包）"""
        if getattr(sys, 'frozen', False):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(base_path, relative_path)
        
    @staticmethod
    def _setup_java_env(silent=False):
        """寻找并配置 Java 环境变量"""
        # 如果 .env 中强制指定了 JAVA_HOME，则优先使用
        env_java_home = os.getenv("JAVA_HOME_OVERRIDE")
        if env_java_home and os.path.exists(env_java_home):
            os.environ['JAVA_HOME'] = env_java_home
            return True

        potential_paths= [
            r"\\172.18.250.2\中段工艺\98_Common\吴广\jdk-21_windows-x64_bin",
            r"\\172.18.250.2\shast document\5140_MFG3\05_RD\001_study report\吴广_RD\jdk-21_windows-x64_bin",
            r"\\172.18.250.2\shast document\5140_MFG3\02_Polishing\003_Pesonal\吴广\jdk-21_windows-x64_bin"
        ]
        
        java_home_path = next((p for p in potential_paths if os.path.exists(p)), None)
        
        if not java_home_path or not os.path.exists(java_home_path):
            if not silent:
                messagebox.showerror("Java Environment Error", "指定的 Java 路径不存在，请检查网络驱动器或配置。")
            return False
            
        os.environ['JAVA_HOME'] = java_home_path
        return True

    @staticmethod
    def get_db_connection(silent=False):
        """获取数据库连接"""
        if not DatabaseManager._setup_java_env(silent):
            return None
        
        # 使用来自 config.py 的配置，不暴露真实密码
        jar_path = DatabaseManager.get_resource_path(os.path.join("Driver", "db2jcc4.jar"))
        
        if not os.path.exists(jar_path):
            if not silent:
                messagebox.showerror("Error", f"数据库驱动文件缺失，请检查:\n{jar_path}")
            return None

        try:
            conn = jaydebeapi.connect(
                jclassname=Config.DB_DRIVER_NAME,
                url=Config.JDBC_URL,
                driver_args=[Config.DB_UID, Config.DB_PWD],
                jars=jar_path
            )
            return conn
        except Exception as e:
            if not silent:
                messagebox.showerror("数据库连接失败", f"无法连接到数据库。请检查 VPN、账号密码配置 (.env)。\n\n报错详情: {e}")
            else:
                print(f"Background DB connection attempt failed: {e}")
            return None

