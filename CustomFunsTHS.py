from setup import *
from iFinDPy import THS_DP, THS_HQ, THS_BD


def convert_date_format_from_8_to_10(x):
    return "{}-{}-{}".format(x[0:4], x[4:6], x[6:8])


def download_security_id_by_date_ths(t_download_date: str, t_save_root_dir: str, t_skip_when_exists: bool):
    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
    save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
    save_file = "{}.cne.csv.gz".format(t_download_date)
    save_path = os.path.join(save_dir, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        downloaded_data = THS_DP("block", "{};001005010".format(convert_date_format_from_8_to_10(t_download_date)), "thscode:Y")
        if downloaded_data.errorcode == 0:
            downloaded_df = pd.DataFrame({"wind_code": [z for z in downloaded_data.data["THSCODE"] if z[-2:] != "BJ"]})
            downloaded_df = downloaded_df.sort_values(by="wind_code", ascending=True)
            downloaded_df.to_csv(save_path, index=False)
            print("| {0} | {1} | {2:>7d} sec_id downloaded successfully |".format(dt.datetime.now(), t_download_date, len(downloaded_df)))
        else:
            print("| {0} | {1} | Error when download security id, Error Code = {2} |".format(dt.datetime.now(), t_download_date, downloaded_data.errorcode))

    return 0


def download_security_mkt_data_by_date_ths(t_download_date: str, t_save_root_dir: str, t_sec_id_list: list, t_skip_when_exists: bool):
    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
    save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
    save_file = "{}.cne.md.csv.gz".format(t_download_date)
    save_path = os.path.join(save_dir, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        downloaded_data = THS_HQ(
            t_sec_id_list,
            "open,high,low,close,volume,preClose",
            "",
            convert_date_format_from_8_to_10(t_download_date),
            convert_date_format_from_8_to_10(t_download_date)
        )

        if downloaded_data.errorcode == 0:
            merged_df = downloaded_data.data
            merged_df["pct_chg"] = np.round((merged_df["close"] / merged_df["preClose"] - 1) * 100, 4)
            merged_df["volume"] = merged_df["volume"].fillna(0)
            merged_df["wind_code"] = merged_df["thscode"]
            merged_df = merged_df[["wind_code", "open", "high", "low", "close", "volume", "pct_chg"]]
            merged_df = merged_df.sort_values(by="wind_code", ascending=True)
            merged_df.to_csv(save_path, index=False, float_format="%.4f", compression="gzip")
            print("| {0} | {1} | market data {2:>12s} | downloaded successfully |".format(dt.datetime.now(), t_download_date, "ohlcvpc"))
        else:
            print("| {0} | {1} | Error when download security md, Error Code = {2} |".format(dt.datetime.now(), t_download_date, downloaded_data.errorcode))

    return 0


def download_security_mkt_data_extra_by_date_ths(t_download_date: str, t_save_root_dir: str, t_sec_id_list: list, t_extra_var: str, t_skip_when_exists: bool):
    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
    save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
    save_file = "{}.cne.md.{}.csv.gz".format(t_download_date, t_extra_var)
    save_path = os.path.join(save_dir, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        t_extra_var_ths = {
            "money": "amount"
        }[t_extra_var]
        downloaded_data = THS_HQ(
            t_sec_id_list,
            t_extra_var_ths,
            "",
            convert_date_format_from_8_to_10(t_download_date),
            convert_date_format_from_8_to_10(t_download_date)
        )
        if downloaded_data.errorcode == 0:
            merged_df = downloaded_data.data
            merged_df["wind_code"] = merged_df["thscode"]
            merged_df[t_extra_var] = merged_df[t_extra_var_ths]
            merged_df = merged_df[["wind_code", t_extra_var]]
            merged_df = merged_df.sort_values(by="wind_code", ascending=True)
            merged_df.to_csv(save_path, index=False, float_format="%.4f", compression="gzip")
            print("| {0} | {1} | market data {2:>12s} | downloaded successfully |".format(dt.datetime.now(), t_download_date, t_extra_var))
        else:
            print("| {0} | {1} | Error when download security {2}, Error Code = {3} |".format(dt.datetime.now(), t_download_date, t_extra_var, downloaded_data.errorcode))
    return 0


def download_security_fundamental_by_date_ths(t_download_date: str, t_save_root_dir: str, t_sec_id_list: list, t_variable: str, t_skip_when_exists: bool):
    # available t_variable
    rename_mapper_ths_to_generic = {
        "ths_mv_back_test_stock": "mkt_cap",
        "ths_current_mv_stock": "clc_mkt_cap",
        "ths_pb_mrq_stock": "pb",
        "ths_pe_ttm_stock": "pe",
        "ths_turnover_ratio_stock": "to_rto"
    }
    rename_mapper_generic_to_ths = {v: k for k, v in rename_mapper_ths_to_generic.items()}

    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
    save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
    save_file = "{}.cne.{}.csv.gz".format(t_download_date, t_variable)
    save_path = os.path.join(save_dir, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        downloaded_df = None
        t_variable_ths = rename_mapper_generic_to_ths.get(t_variable, None)
        if t_variable_ths is not None:
            downloaded_data = THS_BD(t_sec_id_list, t_variable_ths, convert_date_format_from_8_to_10(t_download_date))
            if downloaded_data.errorcode == 0:
                downloaded_df = downloaded_data.data
            else:
                print("| {0} | {1} | Error when download security {2:}, Error Code = {3} |".format(
                    dt.datetime.now(), t_download_date, t_variable, downloaded_data.errorcode))

        if downloaded_df is not None:
            downloaded_df = downloaded_df.rename(mapper=rename_mapper_ths_to_generic, axis=1)
            downloaded_df["wind_code"] = downloaded_df["thscode"]
            downloaded_df = downloaded_df[["wind_code", t_variable]].sort_values(by="wind_code", ascending=True)
            save_precision = {
                "mkt_cap": "%.2f",
                "clc_mkt_cap": "%.2f",
                "pb": "%.6f",
                "pe": "%.6f",
                "to_rto": "%.6f",
            }[t_variable]
            downloaded_df.to_csv(save_path, index=False, float_format=save_precision, compression="gzip")
            print("| {0} | {1} | market data {2:>12s} | downloaded successfully |".format(dt.datetime.now(), t_download_date, t_variable))

    return 0


def download_security_sector_by_date_ths(t_download_date: str, t_save_root_dir: str, t_sec_id_list: list, t_sector_class: str, t_skip_when_exists: bool):
    sector_sub_class_details = {
        "sw_l1": ("ths_the_sw_industry_index_code_stock", "ths_the_sw_industry_stock", "100"),
        "sw_l2": ("ths_the_sw_industry_index_code_stock", "ths_the_sw_industry_stock", "101"),
        "sw_l3": ("ths_the_sw_industry_index_code_stock", "ths_the_sw_industry_stock", "102"),
        "zjw_l1": ("ths_the_new_csrc_industry_code_stock", "ths_the_new_csrc_industry_stock", "100"),
        "zjw_l2": ("ths_the_new_csrc_industry_code_stock", "ths_the_new_csrc_industry_stock", "101"),
    }
    sector_sub_class_mgr = {
        "sw_l1": ["sw_l1"],
        "sw_l2": ["sw_l2"],
        "sw_l3": ["sw_l3"],
        "zjw": ["zjw_l1", "zjw_l2"],
    }

    download_year = t_download_date[0:4]
    check_and_mkdir(os.path.join(t_save_root_dir, download_year))
    check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
    save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
    save_file = "{}.cne.sector.{}.csv.gz".format(t_download_date, t_sector_class)
    save_path = os.path.join(save_dir, save_file)

    if os.path.exists(save_path) and t_skip_when_exists:
        pass
    else:
        sector_dfs_mgr = {}
        for sector_sub_class in sector_sub_class_mgr[t_sector_class]:
            sector_class_v = sector_sub_class_details[sector_sub_class]
            indicators = ";".join(sector_class_v[0:2])
            parameters = "{0},{1};{0},{1}".format(sector_class_v[2], convert_date_format_from_8_to_10(t_download_date))
            downloaded_data = THS_BD(t_sec_id_list, indicators, parameters)
            if downloaded_data.errorcode == 0:
                sector_dfs_mgr[sector_sub_class] = downloaded_data.data.rename(
                    {
                        "thscode": "wind_code",
                        sector_class_v[0]: "industry_code",
                        sector_class_v[1]: "industry_name",
                    },
                    axis=1
                )
            else:
                print("| {0} | {1} | Error when download security sector {2:}, Error Code = {3} |".format(
                    dt.datetime.now(), t_download_date, sector_sub_class, downloaded_data.errorcode))

        if t_sector_class != "zjw":
            sector_df = sector_dfs_mgr[sector_sub_class_mgr[t_sector_class][0]]
        else:
            sector_df = pd.merge(
                left=sector_dfs_mgr[sector_sub_class_mgr[t_sector_class][0]], right=sector_dfs_mgr[sector_sub_class_mgr[t_sector_class][1]],
                on="wind_code", how="left",
                suffixes=("_1", "_2")
            )
            sector_df["industry_code"] = sector_df.apply(lambda z: z["industry_code_1"] + z["industry_code_2"], axis=1)
            sector_df["industry_name"] = sector_df["industry_name_2"]

        # fill nan name
        mapper_name_to_code = sector_df.loc[sector_df["industry_code"] != "", ["industry_code", "industry_name"]] \
            .drop_duplicates(keep="first").set_index("industry_name")["industry_code"].to_dict()
        sector_df["industry_code"] = sector_df[["industry_code", "industry_name"]].apply(
            lambda z: mapper_name_to_code.get(z["industry_name"], "") if z["industry_code"] == "" else z["industry_code"], axis=1)

        sector_df = sector_df[["wind_code", "industry_code", "industry_name"]].sort_values(by="wind_code", ascending=True)
        sector_df.to_csv(save_path, index=False, encoding="gb18030")
        print("| {0} | {1} | market data {2:>12s} | downloaded successfully |".format(dt.datetime.now(), t_download_date, t_sector_class))
    return 0

# def download_dragon_and_tiger_jq(t_download_date: str, t_save_root_dir: str, t_skip_when_exists: bool):
#     download_year = t_download_date[0:4]
#     check_and_mkdir(os.path.join(t_save_root_dir, download_year))
#     check_and_mkdir(os.path.join(t_save_root_dir, download_year, t_download_date))
#     save_dir = os.path.join(t_save_root_dir, download_year, t_download_date)
#     save_file = "{}.cne.dragon_and_tiger.csv.gz".format(t_download_date)
#     save_path = os.path.join(save_dir, save_file)
#     if os.path.exists(save_path) and t_skip_when_exists:
#         pass
#     else:
#         i = 0
#         max_try_num = 10
#         while i < max_try_num:
#             df = get_billboard_list(stock_list=None, end_date=convert_date_format_from_8_to_10(t_download_date), count=1)  # type:pd.DataFrame
#             if len(df) > 0:
#                 df = df.rename(mapper={"code": "wind_code"}, axis=1)
#                 df["wind_code"] = df["wind_code"].map(convert_security_id_from_jq_to_wind)
#                 df.to_csv(save_path, index=False, float_format="%.4f", compression="gzip", encoding="gb18030")
#                 print("| {2} | {0} | market data {1:>12s} | downloaded successfully |".format(t_download_date, "dragon_tiger", dt.datetime.now()))
#                 break
#             i += 1
#             print("| {} | Failed for the {:>2d} time. |".format(dt.datetime.now(), i))
#             time.sleep(5)
#         return 0
