from CustomClass import *
from CustomFunsTHS import *
from CustomFunsDFCF import download_forbidden_release_dfcf
from CustomFunsSZSE import download_szse_public_info, parse_szse_public_info
from CustomFunsSSE import download_sse_public_info, parse_sse_public_info

download_date = sys.argv[1]
user_name = sys.argv[2]
skip_when_exists = sys.argv[3].upper() in ["T", "TRUE"]

# --- load calendar
cne_calendar = CCalendar(SKYRIM_CONST_CALENDAR_PATH)

# --- account
run_account = CTHSAccount(
    t_account_id=THS_ACCOUNT_TABLE[user_name]["account_id"],
    t_password=THS_ACCOUNT_TABLE[user_name]["password"],
)
run_account.log_in()
run_account.inquire_and_display_quotes()

# --- download security id
download_security_id_by_date_ths(download_date, EQUITY_SECURITY_ID_DIR, skip_when_exists)
sec_id_list = CSecIdGrp(EQUITY_SECURITY_ID_DIR, download_date).get_id_list()
# sec_id_list = ["000001.SZ", "600000.SH", "601127.SH", "002074.SZ"]

# md
download_security_mkt_data_by_date_ths(download_date, EQUITY_SECURITY_MKT_DATA_DIR, sec_id_list, skip_when_exists)
download_security_mkt_data_extra_by_date_ths(download_date, EQUITY_SECURITY_MKT_DATA_DIR, sec_id_list, "money", skip_when_exists)

# fm: mkt_cap
download_security_fundamental_by_date_ths(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_sec_id_list=sec_id_list,
    t_variable="mkt_cap",
    t_skip_when_exists=skip_when_exists
)

# fm: circulating_mkt_cap
download_security_fundamental_by_date_ths(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_sec_id_list=sec_id_list,
    t_variable="clc_mkt_cap",
    t_skip_when_exists=skip_when_exists
)

# fm: pb
download_security_fundamental_by_date_ths(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_sec_id_list=sec_id_list,
    t_variable="pb",
    t_skip_when_exists=skip_when_exists
)

# fm: pe
download_security_fundamental_by_date_ths(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_sec_id_list=sec_id_list,
    t_variable="pe",
    t_skip_when_exists=skip_when_exists
)

# fm: rt_rto
download_security_fundamental_by_date_ths(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_sec_id_list=sec_id_list,
    t_variable="to_rto",
    t_skip_when_exists=skip_when_exists
)

# download sectors: Shenwan level 1
download_security_sector_by_date_ths(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_sec_id_list=sec_id_list,
    t_sector_class="sw_l1",
    t_skip_when_exists=skip_when_exists
)

# sectors: Shenwan level 2
download_security_sector_by_date_ths(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_sec_id_list=sec_id_list,
    t_sector_class="sw_l2",
    t_skip_when_exists=skip_when_exists
)

# sectors: Shenwan level 3
download_security_sector_by_date_ths(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_sec_id_list=sec_id_list,
    t_sector_class="sw_l3",
    t_skip_when_exists=skip_when_exists
)

# sectors: CSRC
download_security_sector_by_date_ths(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_sec_id_list=sec_id_list,
    t_sector_class="zjw",
    t_skip_when_exists=skip_when_exists
)

# forbidden release
download_forbidden_release_dfcf(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_skip_when_exists=skip_when_exists,
)

# public info - SZSE
download_szse_public_info(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_skip_when_exists=skip_when_exists
)
parse_szse_public_info(
    t_report_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR
)

# public info - SSE
download_sse_public_info(
    t_download_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR,
    t_skip_when_exists=skip_when_exists
)
parse_sse_public_info(
    t_report_date=download_date,
    t_save_root_dir=EQUITY_SECURITY_MKT_DATA_DIR
)

print("| {} | {} | the following quota remains after download  |".format(dt.datetime.now(), download_date))
run_account.inquire_and_display_quotes()
