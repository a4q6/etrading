if "get_ipython" in globals():
    from IPython import display, HTML
    ipython = get_ipython()
    ipython.run_line_magic("load_ext", "autoreload")
    ipython.run_line_magic("autoreload", "2")

import sys, os
import time, datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import json
from pandas import IndexSlice as ix
from glob import glob
from statsmodels.api import OLS
from IPython.display import HTML
from pathlib import Path
from tqdm.auto import tqdm
from IPython import get_ipython

ip = get_ipython()
if ip is not None:
    ip.run_line_magic("load_ext", "autoreload")
    ip.run_line_magic("autoreload", "2")

pd.set_option("display.max_columns", None)

plt.rcParams.update({"mathtext.fontset": "stix"})
# plt.rcParams.update({"font.family":"Times New Roman"})
plt.rcParams.update({"font.size": 15})
plt.rcParams.update({"axes.grid": True})
plt.rcParams.update({"grid.color": "gray"})
plt.rcParams.update({"grid.linewidth": 0.5})
plt.rcParams.update({"grid.linestyle": "dotted"})
plt.rcParams.update({"xtick.labelsize": 14})  # 横軸のフォントサイズ
plt.rcParams.update({"ytick.labelsize": 14})  # 縦軸のフォントサイズ
plt.rcParams.update({"lines.linewidth": 0.8})
plt.rcParams.update(
    {"date.autoformatter.hour": "%m/%d %H",
     "date.autoformatter.minute": "%H:%M"}
)
plt.rcParams["figure.figsize"] = (18, 12)
plt.rcParams["figure.figsize"] = (10, 6)
full_width_display = "<style>.container { width:80% !important; }</style>"
