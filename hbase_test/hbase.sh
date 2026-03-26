echo `date` "HBase测试开始"
/usr/local/hbase/bin/start-hbase.sh
sleep 60
# hbase 随机读测试
hbase org.apache.hadoop.hbase.PerformanceEvaluation --size=100 --table=Test_200Region_100GB --presplit=200 randomWrite 30
echo "flush 'Test_200Region_100GB'" | hbase shell
echo "major_compact 'Test_200Region_100GB'" | hbase shell
sleep 1800
hbase org.apache.hadoop.hbase.PerformanceEvaluation --nomapred --rows=990000 --table=Test_200Region_100GB randomRead 100 > hbase_randomread_100GB_200Region_allcache_0.log 2>&1
hbase org.apache.hadoop.hbase.PerformanceEvaluation --nomapred --rows=990000 --table=Test_200Region_100GB randomRead 100 > hbase_randomread_100GB_200Region_allcache_1.log 2>&1
hbase org.apache.hadoop.hbase.PerformanceEvaluation --nomapred --rows=990000 --table=Test_200Region_100GB randomRead 100 > hbase_randomread_100GB_200Region_allcache_2.log 2>&1
hbase org.apache.hadoop.hbase.PerformanceEvaluation --nomapred --rows=990000 --table=Test_200Region_100GB randomRead 100 > hbase_randomread_100GB_200Region_allcache_3.log 2>&1
echo "disable 'Test_200Region_100GB'" | hbase shell
echo "drop 'Test_200Region_100GB'" | hbase shell
# hbase 随机写测试
hbase org.apache.hadoop.hbase.PerformanceEvaluation --nomapred --size=1024 --table=Perform_Test --presplit=60 --compress='SNAPPY' randomWrite 120 > hbase_randomwrite_1T_60Region_1.log 2>&1
hbase org.apache.hadoop.hbase.PerformanceEvaluation --nomapred --size=1024 --table=Perform_Test --presplit=60 --compress='SNAPPY' randomWrite 120 > hbase_randomwrite_1T_60Region_2.log 2>&1
hbase org.apache.hadoop.hbase.PerformanceEvaluation --nomapred --size=1024 --table=Perform_Test --presplit=60 --compress='SNAPPY' randomWrite 120 > hbase_randomwrite_1T_60Region_3.log 2>&1
/usr/local/hbase/bin/stop-hbase.sh
# hbase bulkload测试
# pass
