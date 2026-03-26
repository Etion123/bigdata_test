#!/usr/bin/env bash
source /etc/profile
source ~/.bashrc
# set -aux


# 定义家目录
if [ -z "${omni_home}" ]; then
    export omni_home="$(cd "`dirname "$0"`"/..;pwd)"
fi
chmod -R +x ${omni_home}

# 设置单个任务的超时时间
timeout=4h
# sql文件绝对路径
sqldir=${omni_home}/sqls/spark-queries-tpcds
# 数据集名称
data_base=tpcds_bin_partitioned_orc_1000
# 集群主机名
server_list=(vcloud-10-13)
# 耗时报告
sparkreport_file=$omni_home/report/hibench-spark.report
mkdir -p ${omni_home}/report
# spark版本号
spark_version=spark-`readlink -f $SPARK_HOME | awk -F'-' '{{print $2}}'`

echo "-----------------------------分界线-----------------------------" >>$sparkreport_file
spark_native=(
--deploy-mode client
--driver-cores 4
--driver-memory 8g
--master yarn
--executor-cores 8
--executor-memory 14g
--num-executors 31
--conf spark.executor.extraJavaOptions='-XX:+UseG1GC -XX:+UseNUMA'
--conf spark.locality.wait=0
--conf spark.network.timeout=600
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.autoBroadcastJoinThreshold=30M
--conf spark.sql.broadcastTimeout=600
--conf spark.sql.shuffle.partitions=600
--conf spark.sql.orc.impl=native
--conf spark.executor.memoryOverhead=200m
--conf spark.task.cpus=1
--conf spark.locality.wait=0
)

# get 运行时间
function spark_gettime() {
    local res=$1
    local result_file=$2
    local log_file=$3
    local rugular_marchNum='^[0-9]+([.][0-9]+)?$'
    if [[ $res == 0 ]]; then
        local RESULT=$(cat "$log_file" | grep "^Time taken:" | tail -1 | awk '{print $(3)}')
        if ! [[ ${RESULT} =~ ${rugular_marchNum} ]]; then
            echo "Error"
        else
            echo "${RESULT}"
        fi
    else
        echo "Error"
    fi
}

# saprk原生执行query测试
function spark_run_native() {
    local file
    local sql=$1
    mkdir -p ${omni_home}/logs/spark
    mkdir -p ${omni_home}/report
    for file in $(ls $sqldir/${sql}*.sql | sort -u); do
        local fullname=${file##*/}
        #local a=`echo $fullname | grep -o '[0-9]\+'`
        #if [ $a -ge 55 ]; then
        local filename=${fullname%.*}
        local runname=native_${filename}
        local spark_res=${omni_home}/logs/spark/native_${filename}.result
        local spark_log=${omni_home}/logs/spark/native_${filename}.log
        # if [ -f ${sqldir}/${filename}.setting ]; then
        #     local set=$(cat ${sqldir}/${filename}.setting)
        # fi
        # clear_cache
        #run_nmon
        timeout $timeout $SPARK_HOME/bin/spark-sql "${spark_native[@]}" --database $data_base --name ${spec}_${runname} -f $file 1>$spark_res 2>$spark_log
        ret=$?
        #close_nmon
        local sparkcost_time=$(spark_gettime "$ret" "${spark_res}" "${spark_log}")
        local rows=`printf "%8d" $(cat ${spark_log} | grep '^Time taken: .* seconds, Fetched .* row(s)' | awk '{print $6}')`
        local app_id=$(cat ${spark_log} | grep -o "application_.*" | grep -v " " | grep -v "/" | grep -v "\." | head -n 1)
        echo "Time: $(date "+%Y-%m-%d %T.%3N") ${spark_version} ${data_base} ${app_id} $runname Fetched_rows: ${rows} cost_seconds:   ${sparkcost_time}" >>${sparkreport_file}
        #fi
    done
}

# nmon采集所有节点数据
function run_nmon() {
    local arch=$(lscpu | grep Architecture | awk '{print $2}')
    if [ X"${arch}" = "Xx86_64" ]; then
        for i in ${server_list[*]}; do
            ssh ${i} "ps -ef | grep -v grep | grep nmon |awk '{print \$2}' | xargs kill -9"
            scp -rp ${omni_home}/conf/nmon-x86/nmon ${i}:~/
            # 采集服务器mnon数据、60s一次、采集1440次、共24h、频率过高解析数据会卡
            ssh ${i} "nohup cd ~;chmod +x nmon && ./nmon -f -t -m /root -s 5 -c 1440 >/dev/null 2>&1 &"
        done
    else
        for i in ${server_list[*]}; do
            ssh ${i} "ps -ef | grep -v grep | grep nmon |awk '{print \$2}' | xargs kill -9"
            scp -rp ${omni_home}/conf/nmon-arm/nmon ${i}:~/
            ssh ${i} "nohup cd ~;chmod +x nmon && ./nmon -f -t -m /root -s 5 -c 1440 >/dev/null 2>&1 &"
        done
    fi
}

# 停止nmon采集数据，传回数据到目录${omni_home}/report/nmon
function close_nmon() {
    local arch=$(lscpu | grep Architecture | awk '{print $2}')
    for i in ${server_list[*]}; do
        mkdir -p ${omni_home}/report/${nmon_dir}/${runname}
        scp -rp ${i}:~/*.nmon ${omni_home}/report/${nmon_dir}/${runname}/${runname}_${i}.nmon
        if [ X"${arch}" = "Xx86_64" ]; then
            ${omni_home}/conf/nmon-x86/nmonchart ${omni_home}/report/${nmon_dir}/${runname}/${runname}_${i}.nmon ${omni_home}/report/${nmon_dir}/${runname}/${runname}_${i}.html
        else
            ${omni_home}/conf/nmon-arm/nmonchart ${omni_home}/report/${nmon_dir}/${runname}/${runname}_${i}.nmon ${omni_home}/report/${nmon_dir}/${runname}/${runname}_${i}.html
        fi
        ssh ${i} "rm ~/*nmon -rf"
        ssh ${i} "ps -ef | grep -v grep | grep nmon |awk '{print \$2}' | xargs kill -9"
    done
}

# 清理所有节点缓存
function clear_cache() {
    for i in ${server_list[*]}; do
        ssh $i "sync;echo 3 > /proc/sys/vm/drop_caches"
    done
}

function main() {
    usage="Usage: bash ${0##*/} [spark/omni/all(--testmode 可选默认omni)] [1(--times 可选默认1)] [q10(--sql 可选默认空)]"
    local RunArgs=$1
    local Times=$2
    local sql=$3
    local spec=$4
    local TIME_CUR=$(date "+%Y-%m-%d-%H-%M-%S-%3N")
    if [ -z $1 ]; then
        RunArgs="all"
    fi
    if [ -z $2 ]; then
        Times=1
    fi
    if [ -z $3 ]; then
        sql=""
    fi
    if [ -z $4 ]; then
        spec='default'
    fi
    #clear_cache
    if [ ${RunArgs} = "spark" ]; then
        for i in $(seq 1 $Times); do
            nmon_dir=nmon_$(date +%Y%m%d%H%M%S)_${spec}
            #run_nmon
            spark_run_native $sql
            #close_nmon
        done
    elif [ ${RunArgs} = "nmonclose" ]; then
         close_nmon
    elif [ ${RunArgs} = "-h" ]; then
        echo $usage
    else
        for i in $(seq 1 $Times); do
            nmon_dir=nmon_$(date +%Y%m%d%H%M%S)_${spec}
            run_nmon
            spark_run_omni $sql
            mkdir -p ${omni_home}/logs/spark_${TIME_CUR}
            /bin/cp -rp ${omni_home}/logs/spark/* ${omni_home}/logs/spark_${TIME_CUR} 2>&1 &
            close_nmon
        done
    fi
}

main "$@"
