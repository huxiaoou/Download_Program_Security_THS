import os
import sys
import time
import random
import datetime as dt
import numpy as np
import pandas as pd
import requests
import json
from skyrim.whiterun import CCalendar
from skyrim.winterhold import check_and_mkdir
from skyrim.configurationOffice import SKYRIM_CONST_CALENDAR_PATH
# from skyrim.configurationHome import SKYRIM_CONST_CALENDAR_PATH

pd.set_option("display.width", 0)
pd.set_option("display.float_format", "{:.2f}".format)

EQUITY_DIR = os.path.join("E:\\", "Database", "Equity")
# EQUITY_DIR = os.path.join("F:\\", "Database", "Equity")
# EQUITY_DIR = os.path.join("G:\\", "Database", "Equity")

EQUITY_SECURITY_ID_DIR = os.path.join(EQUITY_DIR, "security_id")
EQUITY_SECURITY_MKT_DATA_DIR = os.path.join(EQUITY_DIR, "security_mkt_data")

SEP_LINE = "=" * 120
