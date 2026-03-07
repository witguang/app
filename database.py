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
            r"\\172.18.250.2\中段工艺\98_Common\吴广\jdk-21_windows-x64_bin",
            r"\\172.18.250.2\shast document\5140_MFG3\05_RD\001_study report\吴广_RD\jdk-21_windows-x64_bin",
            r"\\172.18.250.2\shast document\5140_MFG3\02_Polishing\003_Pesonal\吴广\jdk-21_windows-x64_bin"
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

        # 核心防护：加锁！阻止多个线程同时同时试图去拷贝 JDK 或拉起 JVM
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