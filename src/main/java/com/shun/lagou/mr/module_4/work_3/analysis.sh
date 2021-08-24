today=$(date "+%Y%m%d")
source ~/.bashrc
hive -e 'insert into user_info
          (active_num, dataStr)
          values
          (select count(1) from user_clicks group by id, $today)'