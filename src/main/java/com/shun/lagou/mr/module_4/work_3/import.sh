today=$(date "+%Y%m%d")
source ~/.bashrc
hive -e 'load data inpath "data/$today/clicklog.dat" into table user_clicks partition(dt=$today);'