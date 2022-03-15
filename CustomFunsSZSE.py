from setup import *
from CustomClass import *

"""
0.  download Public Information from Shenzhen
"""


# --- for SZSE
def is_title_line_szse(t_net_line: str) -> bool:
    return re.search("\(代码[0-9]{6}\)", t_net_line) is not None


def download_szse_public_info(t_download_date: str, t_save_root_dir: str, t_skip_when_exists: bool):
    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))

    # set headers
    browser_headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36",
        "referer": "http://www.szse.cn/disclosure/deal/public/",
    }

    for block_type in ["a", "c"]:
        save_file = "{}.public_info.SZ-{}.txt".format(t_download_date, block_type)
        save_path = os.path.join(t_save_root_dir, download_year, t_download_date, save_file)

        if os.path.exists(save_path) and t_skip_when_exists:
            pass
        else:
            # download link
            ts = str(int(time.time() * 1000))
            download_link = {
                "a": "http://reportdocs.static.szse.cn/files/text/jy/jy{1}.txt?_={0}".format(ts, t_download_date[2:]),
                "c": "http://reportdocs.static.szse.cn/files/text/nmTxT/GK/nm_jy{1}.txt?_={0}".format(ts, t_download_date[2:]),
            }[block_type]

            # get response
            response = requests.get(url=download_link, headers=browser_headers)
            with open(save_path, mode="w+", encoding="utf-8") as f:
                f.write(response.text)

            print("| {0} | {1} | public info from SZ-{2} downloaded |".format(dt.datetime.now(), t_download_date, block_type.upper()))
            time.sleep(3)
    return 0


def parse_szse_public_info(t_report_date: str, t_save_root_dir: str):
    dfs_list = []
    for block_type in ["a", "c"]:
        raw_file = "{}.public_info.SZ-{}.txt".format(t_report_date, block_type)
        raw_path = os.path.join(t_save_root_dir, t_report_date[0:4], t_report_date, raw_file)
        if not os.path.exists(raw_path):
            print("Raw file for public info of SZSE-{} at {} does not exists, please check again.".format(block_type.upper(), t_report_date))
        else:
            with open(raw_path, "r", encoding="utf-8") as f:
                # find all useful lines
                detailed_info = False
                net_lines_book = []
                for raw_line in f.readlines():
                    net_line = raw_line.replace("\n", "")

                    if len(net_line.replace(" ", "")) == 0:
                        continue

                    if net_line[0:4] == "详细信息":
                        detailed_info = True
                        continue

                    if detailed_info:
                        net_lines_book.append(net_line)

                # parse useful line
                next_line_is_block_description = False
                block_description = ""
                record_description = ""
                block_manager = CBlockManager(t_market="SZSE")
                p_trade_block = None
                for net_line in net_lines_book:
                    if net_line[0:20] == "-" * 20:
                        next_line_is_block_description = True
                        continue

                    if next_line_is_block_description:
                        block_description = net_line[:-1]
                        next_line_is_block_description = False
                        continue

                    if is_title_line_szse(t_net_line=net_line):  # "*ST金正(代码002470)              成交量:12329万份/万股  成交金额:34841万元"
                        p_trade_block = CTradeBlock(t_title_line=net_line, t_block_description=block_description, t_market=block_manager.get_market())
                        block_manager.append(t_trade_block=p_trade_block)
                        continue

                    if net_line.find("异常期间") == 0:  # "异常期间:2021/08/25至2021/08/27   累计涨幅偏离值:15.97%"
                        continue

                    if net_line in ["买入金额最大的前5名", "卖出金额最大的前5名"]:  # "买入金额最大的前5名" "卖出金额最大的前5名"
                        record_description = net_line
                        continue

                    if net_line.find("营业部或交易单元名称") == 0:  # "营业部或交易单元名称                                  买入金额(元)  卖出金额(元)"
                        continue

                    if net_line.find("日均换手率与前五个交易日的日均换手率的比值达到30倍，且换手率累计达20%的证券") == 0:
                        block_description += "，日均换手率与前五个交易日的日均换手率的比值达到30倍，且换手率累计达20%的证券"
                        continue

                    p_trade_block.append(t_record_description=record_description, t_content_line=net_line)

                dfs_list.append(block_manager.to_DataFrame())

    parsed_df = pd.concat(dfs_list, ignore_index=True, axis=0)
    parsed_file = "{}.public_info.SZSE.parsed.csv.gz".format(t_report_date)
    parsed_path = os.path.join(t_save_root_dir, t_report_date[0:4], t_report_date, parsed_file)
    parsed_df.to_csv(parsed_path, index=False, float_format="%.2f", encoding="gb18030", compression="gzip")
