import os
from datetime import datetime
from dotenv import load_dotenv

# 自动寻找并加载 .env 文件中的变量到环境变量中
load_dotenv()

class Config:
    # ==========================================
    # 敏感信息及数据库配置 (从 .env 中读取)
    # ==========================================
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "50000")
    DB_NAME = os.getenv("DB_NAME", "MMDB")
    
    # 拼接 JDBC URL
    JDBC_URL = f"jdbc:db2://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    DB_UID = os.getenv("DB_UID", "")
    DB_PWD = os.getenv("DB_PWD", "")
    DB_DRIVER_NAME = os.getenv("DB_DRIVER", "")

    # ==========================================
    # 路径与常量配置 (非敏感，可写死)
    # ==========================================
    PATH_TRANSITION_DATE = datetime.strptime("20250714", "%Y%m%d").date()

    NEW_BASE_PATH = [r"\\172.18.139.4\Analytical_FPMS2", r"\\172.18.139.4\Analytical_Machine2\05_DPGE\02_DPGE101\01_Production"]
    OLD_BASE_PATH = r"\\172.18.139.4\Analytical_Machine2\07_FPMS"
    DPGE101_BASE_PATH = r"\\172.18.139.4\Analytical_Machine2\05_DPGE\02_DPGE101\01_Production"
    
    DEBUG_THICKNESS_SEARCH = False
    
    ERO_PRE_PATH_TEMPLATE = r"\\172.18.139.4\Analytical_Machine2\07_FPMS\00_ERO_PRE\{device}\Success"
    ERO_POST_PATH_TEMPLATE = r"\\172.18.139.4\Analytical_Machine2\07_FPMS\00_ERO_POST\{device}"
    ERO_ERROR_PATH_TEMPLATE = r"\\172.18.139.4\Analytical_Machine2\07_FPMS\00_ERO_ERROR\{device}"
    THK_PROFILE_PATH_TEMPLATE = r"\\172.18.139.4\Analytical_Machine2\07_FPMS\00_THK_Profile\{device}"
    
    IMP_PREFIXES = ["IMP_W26D08_D35S72_EE1", "IMP_W26D08_D35S72_EE2"]
    THICKNESS_PREFIX = "Thickness_"
    SQMM_PREFIXES = ["SQMM-", "SQMM_"]
    THK_SECTOR_PREFIX = "Thickness_Sector_Height_Profile_Sectors_1_Inner_Radius_150mm_(HiRes)-"

    class ThicknessFile:
        WAFER_ID_COL_NAME = "Wafer ID"
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

# 全局绘图参数配置等也可以放在这里，或者放到专门的 utils 里

