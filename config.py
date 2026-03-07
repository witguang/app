import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from datetime import datetime

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

    NEW_BASE_PATH = [r"\\172.18.139.4\Analytical_FPMS2",r"\\172.18.139.4\Analytical_Machine2\05_DPGE\02_DPGE101\01_Production"]
    OLD_BASE_PATH = r"\\172.18.139.4\Analytical_Machine2\07_FPMS"

    DPGE101_BASE_PATH =r"\\172.18.139.4\Analytical_Machine2\05_DPGE\02_DPGE101\01_Production"
    DEBUG_THICKNESS_SEARCH = False
    
    ERO_ERROR_PATH_TEMPLATE = r"\\172.18.139.4\Analytical_Machine2\07_FPMS\00_ERO_ERROR\{device}"
    ERO_POST_PATH_TEMPLATE = r"\\172.18.139.4\Analytical_Machine2\07_FPMS\00_ERO_POST\{device}"
    ERO_PRE_PATH_TEMPLATE = r"\\172.18.139.4\Analytical_Machine2\07_FPMS\00_ERO_PRE\{device}\Success"
    THK_PROFILE_PATH_TEMPLATE = r"\\172.18.139.4\Analytical_Machine2\07_FPMS\00_THK_Profile\{device}"
    
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