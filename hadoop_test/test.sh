# DFSIO 2W个10M文件写测试
echo `date` "DFSIO测试开始"
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -write -nrFiles 20000 -fileSize 10mb > dfsio_20000_10M_write_1.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -write -nrFiles 20000 -fileSize 10mb > dfsio_20000_10M_write_2.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -write -nrFiles 20000 -fileSize 10mb > dfsio_20000_10M_write_3.log 2>&1

# DFSIO 2W个10M文件读测试
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -read -nrFiles 20000 -fileSize 10mb > dfsio_20000_10M_read_1.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -read -nrFiles 20000 -fileSize 10mb > dfsio_20000_10M_read_2.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -read -nrFiles 20000 -fileSize 10mb > dfsio_20000_10M_read_3.log 2>&1

hdfs dfs -rmr /benchmarks

# DFSIO 200个1G文件写测试
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -write -nrFiles 200 -fileSize 1024mb > dfsio_200_1G_write_1.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -write -nrFiles 200 -fileSize 1024mb > dfsio_200_1G_write_2.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -write -nrFiles 200 -fileSize 1024mb > dfsio_200_1G_write_3.log 2>&1

# DFSIO 200个1G文件读测试
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -read -nrFiles 200 -fileSize 1024mb > dfsio_200_1G_read_1.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -read -nrFiles 200 -fileSize 1024mb > dfsio_200_1G_read_2.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -read -nrFiles 200 -fileSize 1024mb > dfsio_200_1G_read_3.log 2>&1

hdfs dfs -rmr /benchmarks

# DFSIO 2000个100M文件写测试
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -write -nrFiles 2000 -fileSize 100mb > dfsio_200_100M_write_1.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -write -nrFiles 2000 -fileSize 100mb > dfsio_200_100M_write_2.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -write -nrFiles 2000 -fileSize 100mb > dfsio_200_100M_write_3.log 2>&1


# DFSIo 2000个100M文件读测试
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -read -nrFiles 2000 -fileSize 100mb > dfsio_200_100M_read_1.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -read -nrFiles 2000 -fileSize 100mb > dfsio_200_100M_read_2.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar TestDFSIO -read -nrFiles 2000 -fileSize 100mb > dfsio_200_100M_read_3.log 2>&1

hdfs dfs -rmr /benchmarks

echo `date` "NNBench测试开始"
# nnbench小文件测试 300个map任务 每个任务1K个文件 每个文件1MB
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar  nnbench -operation create_write -maps 300 -reduces 100 -bytesToWrite 1 -numberOfFiles 1000 -blockSize 1048576 -replicationFactorPerFile 1 -readFileAfterOpen true -baseDir /benchmarks/NNBench > nnbench_300_100_1K_1M_write_1.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar  nnbench -operation create_write -maps 300 -reduces 100 -bytesToWrite 1 -numberOfFiles 1000 -blockSize 1048576 -replicationFactorPerFile 1 -readFileAfterOpen true -baseDir /benchmarks/NNBench > nnbench_300_100_1K_1M_write_2.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar  nnbench -operation create_write -maps 300 -reduces 100 -bytesToWrite 1 -numberOfFiles 1000 -blockSize 1048576 -replicationFactorPerFile 1 -readFileAfterOpen true -baseDir /benchmarks/NNBench > nnbench_300_100_1K_1M_write_3.log 2>&1

hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar  nnbench -operation create_write -maps 300 -reduces 100 -bytesToWrite 1 -numberOfFiles 1000 -blockSize 1048576 -replicationFactorPerFile 1 -readFileAfterOpen true -baseDir /benchmarks/NNBench > nnbench_300_100_1K_1M_read_1.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar  nnbench -operation create_write -maps 300 -reduces 100 -bytesToWrite 1 -numberOfFiles 1000 -blockSize 1048576 -replicationFactorPerFile 1 -readFileAfterOpen true -baseDir /benchmarks/NNBench > nnbench_300_100_1K_1M_read_2.log 2>&1
hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-tests.jar  nnbench -operation create_write -maps 300 -reduces 100 -bytesToWrite 1 -numberOfFiles 1000 -blockSize 1048576 -replicationFactorPerFile 1 -readFileAfterOpen true -baseDir /benchmarks/NNBench > nnbench_300_100_1K_1M_read_3.log 2>&1

hdfs dfs -rmr /benchmarks


# hadoop terasrot 1T数据量测试
echo `date` "Hadoop Terasort测试开始"
/home/HiBench/bin/workloads/micro/terasort/prepare/prepare.sh

/home/HiBench/bin/workloads/micro/terasort/hadoop/run.sh
/home/HiBench/bin/workloads/micro/terasort/hadoop/run.sh
/home/HiBench/bin/workloads/micro/terasort/hadoop/run.sh
/home/HiBench/bin/workloads/micro/terasort/hadoop/run.sh
/home/HiBench/bin/workloads/micro/terasort/hadoop/run.sh

echo `date` "HBase测试开始"
#/usr/local/hbase/bin/start-hbase.sh
sleep 60
# hbase 随机读测试
hbase org.apache.hadoop.hbase.PerformanceEvaluation --size=100 --table=Test_200Region_100GB --presplit=200 randomWrite 30
echo "flush 'Test_200Region_100GB'" | hbase shell
echo "major_compact 'Test_200Region_100GB'" | hbase shell
sleep 1800
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


# 生成数据
echo `date` "生成1T TPC-DS数据"
bash /usr/hive-testbench-hdp3/tpcds-setup.sh 1000 > /usr/hive-testbench-hdp3/genData.log 2>&1

# spark tpc-ds 1T测试
#cd /usr/bigdata_test/spark_test/bin/
#bash spark-run-99-7260.sh spark 3 > spark_tpcds_1T.log 2>&1
#  hive tpc-ds 1T测试
#cd /usr/bigdata_test/hive_test/bin/
#disk_List={a,b,c,d,e,f,g,h}
#for i in ${disk_List}
#do
        # 读取预取块大小为2048KB
#        echo 2048 > /sys/block/sd$i/queue/read_ahead_kb
#done
#bash hive-run.sh hive-default.setting.tpcds 3 > hive_tez_tpcds_1T.log 2>&1
