#!/usr/bin/env bash
source /etc/profile
# set -x


if [ -z "${omni_home}" ]; then
    export omni_home="$(cd "`dirname "$0"`"/..;pwd)"
fi
chmod -R +x ${omni_home}

# sqldir=${omni_home}/sqls/sample-queries-tpch
# data_base=tpch_flat_orc_1000

# sql文件绝对路径
sqldir=${omni_home}/sqls/sample-queries-tpcds-hdp3
# 数据集名称
data_base=tpcds_bin_partitioned_orc_1000
# 集群主机名
server_list=(vcloud-10-13)
# 耗时报告
hivereport_file=$omni_home/report/hibench-hive.report
mkdir -p ${omni_home}/report
# hive版本号
hive_version=hive-`readlink -f $HIVE_HOME | awk -F'-' '{{print $3}}'`

echo "-----------------------------分界线-----------------------------" >>$hivereport_file
hive_run="hive"

# 判断hive不报错并获取运行时间
function hive_runsql() {
    local log_file=$1
    local rugular_marchNum='^[0-9]+([.][0-9]+)?$'
    local res=$2
    if [[ $res == 0 ]]; then
        local RESULT=$(tail "$log_file" | grep "^Time taken:" | tail -1 | awk '{print $(3)}')
        if ! [[ ${RESULT} =~ ${rugular_marchNum} ]]; then
            echo "Error"
        else
            echo "${RESULT}"
        fi
    else
        echo "Error"
    fi
}

# hive测试
function hive_run_native() {
    local file
    local res
    local set=$1
    local sql=$2
    mkdir -p ${omni_home}/logs/hive
    mkdir -p ${omni_home}/report
    for file in $(ls $sqldir/${sql}*.sql | sort -u); do
        local fullname=${file##*/}
        local filename=${fullname%.*}
        local hive_res=${omni_home}/logs/hive/hive_${filename}.result
        local hive_log=${omni_home}/logs/hive/hive_${filename}.log
        local runname=native_${filename}
        run_nmon
        ${hive_run} -i ${set} -database $data_base -f $file 1>${hive_res} 2>${hive_log}
        res=$?
        close_nmon
        local hivecost_time=$(hive_runsql ${hive_log} ${res})
        local rows=`printf "%8d" $(cat ${hive_log} | grep '^Time taken: .* seconds, Fetched: .* row(s)' | awk '{print $6}')`
        echo "Time: $(date "+%Y-%m-%d %T.%3N") $hive_version ${data_base} $filename Fetched_rows: ${rows} cost_seconds:   ${hivecost_time}" >>${hivereport_file}
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
    usage="Usage: bash ${0##*/} [hive-default.setting.tpcds(--conf 必选)] [1(--times 可选默认1)] [q10(--sql 可选默认空)]"
    local RunArgs=$1
    local Times=$2
    local sql=$3
    if [ -z $1 ]; then
        RunArgs=""
    fi
    if [ -z $2 ]; then
        Times=1
    fi
    if [ -z $3 ]; then
        sql=""
    fi
    if [ ${RunArgs} = "-h" ]; then
        echo $usage
    else
        # clear_cache
        for i in $(seq 1 $Times); do
            run_nmon
            hive_run_native $RunArgs $sql
            close_nmon
        done
    fi
}

main "$@"
