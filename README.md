# 抛光数据分析工具

这是一个基于 `Tkinter` 的 Windows 桌面工具，用于抛光 / TOPO / Sublot 追溯 / 报表分析等场景。

当前代码已从早期的单文件 `app.py` 大脚本，拆分为多个职责更清晰的模块，便于维护和继续开发。

## 主要功能

- 基于 `Sublot` 的自动化处理
- 基于 `Product` 的自动化处理
- `TOPO DATA` 数据处理
- `Sublot` 历史追溯
- 数据报表工具

## 目录结构

```text
.
?? app.py                    # compatibility entrypoint
?? main.py                   # current main entrypoint
?? config.py                 # global config and plotting options
?? database.py               # DB connection and JDK/JVM bootstrap
?? data_processor.py         # data processing helpers
?? requirements.txt          # Python dependencies
?? ui/
?  ?? main_window.py         # main window and left navigation
?  ?? topo_tab.py            # TOPO DATA page
?  ?? trace_tab.py           # Sublot trace page
?  ?? report_tab.py          # report page
?  ?? auto_product_tab.py    # product automation page
?  ?? auto_sublot_tab.py     # sublot automation page
?  ?? auto_processing.py     # shared automation/report helpers
?  ?? auto_tab.py            # compatibility export layer
?? icon.ico                  # app icon
```

## 启动方式

推荐使用：

```bash
python main.py
```

兼容旧入口：

```bash
python app.py
```

## 环境要求

建议环境：

- Windows
- Python 3.10+
- 可用的 `Tkinter`
- 可访问数据库所需的网络环境

安装依赖：

```bash
pip install -r requirements.txt
```

## 数据库说明

程序启动后会在后台预热数据库连接，以减少首次查询的等待时间。

数据库相关逻辑位于 `database.py`，主要包括：

- JDK / JVM 初始化
- `jaydebeapi` / `JPype1` 数据库连接
- 连接缓存与重用
- 资源路径解析

如果数据库无法连接，可优先检查：

- Python 依赖是否已安装
- 本机是否具备可用的 JDK / JVM
- 相关网络路径是否可访问
- 数据库驱动和相关资源是否齐全

## 拆分说明

本次整理主要包括：

- 将主窗口类拆分到 `ui/main_window.py`
- 将自动化页面拆分为 Product / Sublot 两个模块
- 将报表与自动化共用函数抽到 `ui/auto_processing.py`
- 保留 `app.py` 和 `ui/auto_tab.py` 作为兼容层
- 修复了部分 UI 中文显示为 `?` 的问题

## 开发建议

如果后续需要修改功能，可优先从以下文件入手：

- 主界面行为：`ui/main_window.py`
- Product 自动化：`ui/auto_product_tab.py`
- Sublot 自动化：`ui/auto_sublot_tab.py`
- 数据报表：`ui/report_tab.py` 和 `ui/auto_processing.py`
- TOPO 逻辑：`ui/topo_tab.py`
- 追溯逻辑：`ui/trace_tab.py`

## 备注

- `.gitignore` 已排除 `__pycache__/` 与 `.claude/`
- `README.md` 未自动提交或推送，如需要我可以继续处理
