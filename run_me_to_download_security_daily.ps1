$trade_date = Get-Date -Format "yyyyMMdd"
python 3_download_security_id_md_fm.py $trade_date htzq12157 247260 T >> .\log\download_security_$trade_date.log
python 8_copy_to_portal.py $trade_date >> .\log\download_security_$trade_date.log
