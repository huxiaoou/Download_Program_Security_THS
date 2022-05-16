from setup import *
from shutil import copy

src_root_dir = EQUITY_DIR
# dst_root_dir = os.path.join("C:\\", "Users", "Administrator", "OneDrive", "文档", "Trading", "Database", "Equity")
dst_root_dir = os.path.join("C:\\", "Users", "huxia", "OneDrive", "文档", "Trading", "Database", "Equity")

trade_date = sys.argv[1]

trade_year = trade_date[0:4]
for data_type in ["security_id", "security_mkt_data"]:
    src_dir = os.path.join(src_root_dir, data_type, trade_year, trade_date)
    dst_dir = os.path.join(dst_root_dir, data_type, trade_year, trade_date)
    if not os.path.exists(src_dir):
        print("Error! Source path does not exist for {} at {}".format(data_type, trade_date))
        continue

    if not os.path.exists(dst_dir):
        os.mkdir(dst_dir)

    for f in os.listdir(src_dir):
        src_path = os.path.join(src_dir, f)
        dst_path = os.path.join(dst_dir, f)
        if os.path.exists(dst_path):
            print("Warning! Destination path: {} already exists at {}".format(dst_path, trade_date))
        else:
            copy(src=src_path, dst=dst_path)

    print("| {2} | {1} | All data of {0:>24s} copied to portal |".format(data_type, trade_date, dt.datetime.now()))
