# 生成数据
#echo `date` "生成1T TPC-DS数据"
#bash /home/hive-testbench-hdp3/tpcds-setup.sh 1000 > /home/hive-testbench-hdp3/genData.log 2>&1

# spark tpc-ds 1T测试
cd /home/bigdata_test/spark_test/bin/
bash spark-run.sh spark 3 > spark_tpcds_1T.log 2>&1
#  hive tpc-ds 1T测试
#cd /home/bigdata_test/hive_test/bin/
#disk_List={a,b,c,d,e,f,g,h}
#for i in ${disk_List}
#do
        # 读取预取块大小为2048KB
#        echo 2048 > /sys/block/sd$i/queue/read_ahead_kb
#done
#bash hive-run.sh hive-default.setting.tpcds 2 > hive_tez_tpcds_1T.log 2>&1


# hadoop terasrot 1T数据量测试
echo `date` "Hadoop Terasort测试开始"
#/home/HiBench-7.0/bin/workloads/micro/terasort/prepare/prepare.sh

#/home/HiBench-7.0/bin/workloads/micro/terasort/hadoop/run.sh
#/home/HiBench-7.0/bin/workloads/micro/terasort/hadoop/run.sh
#/home/HiBench-7.0/bin/workloads/micro/terasort/hadoop/run.sh
#/home/HiBench-7.0/bin/workloads/micro/terasort/hadoop/run.sh
#/home/HiBench-7.0/bin/workloads/micro/terasort/hadoop/run.sh

bash /home/HiBench/bin/workloads/ml/kmeans/prepare/prepare.sh
bash /home/HiBench/bin/workloads/ml/kmeans/spark/run.sh
bash /home/HiBench/bin/workloads/ml/kmeans/spark/run.sh
bash /home/HiBench/bin/workloads/ml/kmeans/spark/run.sh
bash /home/HiBench/bin/workloads/ml/kmeans/spark/run.sh
bash /home/HiBench/bin/workloads/ml/kmeans/spark/run.sh
