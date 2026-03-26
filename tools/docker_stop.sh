mysql_pid=`pidof mysqld`
binlog_file=`tail -n 1 /data1/mysql/data/mysql-bin.index | awk -F '/' '{print $2}'`
mysql -h127.0.0.1 -P3306 -uroot -phuawei -e "purge binary logs to '$binlog_file';shutdown"
tail --pid=$mysql_pid -f /dev/null
docker_id=`docker ps -a | tail -n 1 | awk '{print $1}'`
docker stop $docker_id > /dev/null
docker rm $docker_id > /dev/null
docker ps
