import os
import re
import numpy as np
import pandas as pd
from typing import List
from iFinDPy import THS_iFinDLogin, THS_DataStatistics


class CSecIdGrp(object):
    def __init__(self, t_id_dir: str, t_date: str):
        id_file = "{}.cne.csv.gz".format(t_date)
        id_path = os.path.join(t_id_dir, t_date[0:4], t_date, id_file)
        self.id_df = pd.read_csv(id_path, dtype=str)

    def get_id_list(self):
        return self.id_df["wind_code"].tolist()


class CTHSAccount(object):
    def __init__(self, t_account_id: str, t_password: str):
        self.account_id = t_account_id
        self.password = t_password

    def display(self):
        print("account id   : {}".format(self.account_id))
        return 0

    def log_in(self):
        THS_iFinDLogin(self.account_id, self.password)
        return 0

    def inquire_and_display_quotes(self):
        used_data_summary = THS_DataStatistics()
        quotes_df = pd.DataFrame.from_dict(used_data_summary["tables"], orient="index")
        print("\n")
        print("=" * 120)
        self.display()
        print("-" * 120)
        print(quotes_df)
        print("=" * 120)
        print("\n")
        return 0


# --- classes
class CTradeRecord(object):
    def __init__(self, t_content_line: str, t_market: str, t_record_description: str):
        self.m_member_name: str = ""
        self.m_amt_b: float = np.nan
        self.m_amt_s: float = np.nan

        if t_market == "SZSE":
            words = t_content_line.split()
            self.m_member_name: str = words[0]
            self.m_amt_b: float = float(words[1])
            self.m_amt_s: float = float(words[2])
        elif t_market == "SSE":
            words = t_content_line.split()
            if t_record_description == "买入营业部名称:":
                self.m_member_name: str = words[1]
                self.m_amt_b: float = float(words[2])
                self.m_amt_s: float = np.nan
            elif t_record_description == "融资买入会员名称:":
                self.m_member_name: str = words[1]
                self.m_amt_b: float = float(words[2])
                self.m_amt_s: float = np.nan
            elif t_record_description == "卖出营业部名称:":
                self.m_member_name: str = words[1]
                self.m_amt_b: float = np.nan
                self.m_amt_s: float = float(words[2])

    def to_dict(self):
        return {
            "会员": self.m_member_name,
            "买入金额": self.m_amt_b,
            "卖出金额": self.m_amt_s,
        }


class CTradeBlock(object):
    def __init__(self, t_title_line: str, t_block_description: str, t_market: str):
        self.m_market = t_market
        self.m_block_description: str = ""
        self.m_wind_code = ""
        self.m_sec_name = ""

        if self.m_market == "SZSE":
            bgn_loc, _ = re.search("\(代码[0-9]{6}\)", t_title_line).span()
            self.m_block_description = t_block_description
            self.m_wind_code = t_title_line[bgn_loc + 3:bgn_loc + 9] + ".SZ"
            self.m_chs_name = t_title_line[0:bgn_loc]
        elif self.m_market == "SSE":
            code_loc = t_title_line.find("证券代码: ")
            name_loc = t_title_line.find("证券简称: ")
            self.m_block_description = t_block_description
            self.m_wind_code = t_title_line[code_loc + 6:code_loc + 12] + ".SH"
            self.m_chs_name = t_title_line[name_loc + 6:]

        self.m_max_b_records: List[CTradeRecord] = []
        self.m_max_s_records: List[CTradeRecord] = []

    def append(self, t_record_description: str, t_content_line: str):
        if self.m_market == "SZSE":
            if t_record_description == "买入金额最大的前5名":
                self.m_max_b_records.append(CTradeRecord(t_content_line, t_market=self.m_market, t_record_description=""))
                return 0
            elif t_record_description == "卖出金额最大的前5名":
                self.m_max_s_records.append(CTradeRecord(t_content_line, t_market=self.m_market, t_record_description=""))
                return 0
            else:
                print("| {} | {} | Error! | {} | {} |".format(self.m_wind_code, self.m_chs_name, t_record_description, t_content_line))
                return -1
        elif self.m_market == "SSE":
            if t_record_description == "买入营业部名称:":
                self.m_max_b_records.append(CTradeRecord(t_content_line, t_market=self.m_market, t_record_description=t_record_description))
                return 0
            elif t_record_description == "融资买入会员名称:":
                self.m_max_b_records.append(CTradeRecord(t_content_line, t_market=self.m_market, t_record_description=t_record_description))
                return 0
            elif t_record_description == "卖出营业部名称:":
                self.m_max_s_records.append(CTradeRecord(t_content_line, t_market=self.m_market, t_record_description=t_record_description))
                return 0
            else:
                print("| {} | {} | Error! | {} | {} |".format(self.m_wind_code, self.m_chs_name, t_record_description, t_content_line))
                return -1
        else:
            return -1

    def to_records_list(self):
        records = []
        for i, record in enumerate(self.m_max_b_records):
            d = record.to_dict()
            d.update({
                "异动类型": self.m_block_description,
                "标的代码": self.m_wind_code,
                "标的名称": self.m_chs_name,
                "排名类型": "买入前" + str(i + 1)
            })
            records.append(d)

        for i, record in enumerate(self.m_max_s_records):
            d = record.to_dict()
            d.update({
                "异动类型": self.m_block_description,
                "标的代码": self.m_wind_code,
                "标的名称": self.m_chs_name,
                "排名类型": "卖出前" + str(i + 1)
            })
            records.append(d)

        return records


class CBlockManager(object):
    def __init__(self, t_market: str):
        self.m_market: str = t_market
        self.m_parsed_blocks: List[CTradeBlock] = []

    def append(self, t_trade_block: CTradeBlock):
        self.m_parsed_blocks.append(t_trade_block)
        return 0

    def to_DataFrame(self):
        all_records = []
        for block in self.m_parsed_blocks:
            all_records += block.to_records_list()
        res = pd.DataFrame(all_records)
        res = res[["异动类型", "标的代码", "标的名称", "会员", "排名类型", "买入金额", "卖出金额"]]
        return res

    def get_market(self):
        return self.m_market
