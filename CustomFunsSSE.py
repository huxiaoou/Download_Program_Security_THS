from setup import *
from CustomClass import *

"""
0.  download Public Information from Shanghai
"""


def parse_content_text(t_raw_text: str, t_rid: int):
    j_text = t_raw_text.replace("jsonpCallback{:05d}(".format(t_rid), "{\"content\":")
    j_text = j_text[:-1] + "}"
    d = json.loads(j_text)
    return d["content"]["fileContents"]


# --- for SSE
def is_block_description_sse(t_net_line: str) -> bool:
    big_chs_digit = ["^{}、".format(z) for z in [
        "一", "二", "三", "四", "五", "六", "七", "八", "九", "十",
        "十一", "十二", "十三", "十四", "十五", "十六", "十七", "十八", "十九", "二十"
    ]]
    return re.match("({})".format("|".join(big_chs_digit)), t_net_line) is not None


def remove_big_chs_digit_sse(t_block_description: str):
    big_chs_digit = ["^{}、".format(z) for z in [
        "一", "二", "三", "四", "五", "六", "七", "八", "九", "十",
        "十一", "十二", "十三", "十四", "十五", "十六", "十七", "十八", "十九", "二十"
    ]]
    return re.sub("({})".format("|".join(big_chs_digit)), "", t_block_description)


def is_sub_title_sse(t_net_line: str) -> bool:
    arabic_digit = [
        "1、", "2、", "3、", "4、", "5、",
    ]
    return re.match("({})".format("|".join(arabic_digit)), t_net_line) is not None


def is_table_column_names_sse(t_net_line: str) -> bool:
    # return re.search("证券代码 *证券简称.*成交金额\(万元\)$", t_net_line) is not None
    return re.search("证券代码 *证券简称.*成交金额\(万元\)", t_net_line) is not None


def is_table_item_sse(t_net_line: str) -> bool:
    return re.match("\([0-9]+\) *[0-9]{6} ", t_net_line) is not None


def is_sec_title_line_sse(t_net_line: str) -> bool:
    return re.search("证券代码: [0-9]{6} *证券简称: ", t_net_line) is not None


def is_sec_record_title_sse(t_net_line: str) -> bool:
    sec_record_title = [
        "买入营业部名称:",
        "卖出营业部名称:",
        "融资买入会员名称:",
    ]
    return re.match("({})".format("|".join(sec_record_title)), t_net_line) is not None


def download_sse_public_info(t_download_date: str, t_save_root_dir: str, t_skip_when_exists: bool):
    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))

    save_file = "{}.public_info.SSE.txt".format(t_download_date)
    save_path = os.path.join(t_save_root_dir, t_download_date[0:4], t_download_date, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        # set headers
        browser_headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36",
            "referer": "http://www.sse.com.cn/disclosure/diclosure/public/",
        }

        # download link
        rid = int(random.random() * 1e5)
        ts = str(int(dt.datetime.timestamp(dt.datetime.now()) * 1000))
        download_link = "http://query.sse.com.cn/infodisplay/showTradePublicFile.do?jsonCallBack=jsonpCallback{0:05d}&isPagination=false&dateTx={2}-{3}-{4}&_={1}".format(
            rid, ts, t_download_date[0:4], t_download_date[4:6], t_download_date[6:8]
        )

        # get response
        response = requests.get(url=download_link, headers=browser_headers)

        # parse text
        clean_text = parse_content_text(t_raw_text=response.text, t_rid=rid)

        # save text
        with open(save_path, mode="w+", encoding="utf-8") as f:
            for line in clean_text:
                f.write(line + "\n")

        print("| {0} | {1} | public info from SSE downloaded |".format(dt.datetime.now(), t_download_date))
    return 0


def parse_sse_public_info(t_report_date: str, t_save_root_dir: str):
    raw_file = "{}.public_info.SSE.txt".format(t_report_date)
    raw_path = os.path.join(t_save_root_dir, t_report_date[0:4], t_report_date, raw_file)
    if not os.path.exists(raw_path):
        print("Raw file for public info of SSE at {} does not exists, please check again.".format(t_report_date))
    else:
        dfs_list = []
        with open(raw_path, "r", encoding="utf-8") as f:
            # find all useful lines
            detailed_info = False
            net_lines_book = []
            for raw_line in f.readlines():
                net_line = raw_line.replace("\n", "").strip()

                if len(net_line.replace(" ", "")) == 0:
                    continue

                if net_line[0:4] == "交易日期":
                    detailed_info = True
                    continue

                if detailed_info:
                    net_lines_book.append(net_line)

            # parse useful line
            block_description = ""
            record_description = ""
            block_manager = CBlockManager(t_market="SSE")
            p_trade_block = None

            for net_line in net_lines_book:
                if net_line[0:20] == "-" * 20:
                    continue

                if is_block_description_sse(t_net_line=net_line):  # "一、有价格涨跌幅限制的日收盘价格涨幅偏离值达到7%的前三只证券:"
                    block_description = net_line[:-1]
                    block_description = remove_big_chs_digit_sse(t_block_description=block_description)
                    continue

                if net_line.find("式基金连续三个交易日内累计换手率达到20%") == 0:
                    block_description += net_line
                    continue

                if is_sub_title_sse(t_net_line=net_line):  # "1、A股"
                    continue

                if is_table_column_names_sse(t_net_line=net_line):  # "证券代码      证券简称      偏离值%        成交量        成交金额(万元)"
                    continue

                if is_table_item_sse(t_net_line=net_line):  # "(1)  603033      三维股份       9.43%         8090324           17335.26"
                    continue

                if is_sec_title_line_sse(t_net_line=net_line):  # "证券代码: 603033                                                                    证券简称: 三维股份"
                    p_trade_block = CTradeBlock(t_title_line=net_line, t_block_description=block_description, t_market=block_manager.get_market())
                    block_manager.append(t_trade_block=p_trade_block)
                    continue

                if is_sec_record_title_sse(t_net_line=net_line):
                    record_description = net_line.split()[0]
                    continue

                if (len(net_line) == 1) or (net_line == "上海证券交易所"):  # end of file
                    continue

                p_trade_block.append(t_record_description=record_description, t_content_line=net_line)

            dfs_list.append(block_manager.to_DataFrame())

        parsed_df = pd.concat(dfs_list, ignore_index=True, axis=0)
        parsed_file = "{}.public_info.SSE.parsed.csv.gz".format(t_report_date)
        parsed_path = os.path.join(t_save_root_dir, t_report_date[0:4], t_report_date, parsed_file)
        parsed_df.to_csv(parsed_path, index=False, float_format="%.2f", encoding="gb18030", compression="gzip")
