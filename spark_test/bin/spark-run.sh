#!/usr/bin/env bash
set -eu
source /etc/profile
source ~/.bashrc

if [ -z "${omni_home:-}" ]; then
    export omni_home="$(cd "$(dirname "$0")"/.. && pwd)"
fi

CONFIG_INI="${omni_home}/config.ini"
if [ ! -f "$CONFIG_INI" ]; then
    echo "spark-run.sh: 缺少配置文件 ${CONFIG_INI}" >&2
    exit 1
fi
# shellcheck source=/dev/null
source "$CONFIG_INI"

: "${SPARK_CONCURRENCY:=4}"
: "${TASK_TIMEOUT:=4h}"
: "${DATA_BASE:=tpcds_bin_partitioned_orc_1000}"
: "${SQL_SUBDIR:=sqls/spark-queries-tpcds}"
: "${SPARK_REPORT_REL:=report/hibench-spark.report}"

sqldir="${omni_home}/${SQL_SUBDIR}"
sparkreport_file="${omni_home}/${SPARK_REPORT_REL}"
mkdir -p "${omni_home}/report"

if [ -d "${omni_home}/bin" ]; then
    chmod -R +x "${omni_home}/bin" 2>/dev/null || true
fi

_spark_home_resolved="$(cd -P "${SPARK_HOME}" 2>/dev/null && pwd || echo "${SPARK_HOME}")"
spark_version="spark-$(basename "${_spark_home_resolved}" | awk -F'-' '{print $2}')"
unset _spark_home_resolved

REPORT_LOCK="${omni_home}/report/.hibench-spark.report.lock"
append_report_line() {
    local line=$1
    (
        flock -x 200
        printf '%s\n' "$line" >>"$sparkreport_file"
    ) 200>>"$REPORT_LOCK"
}

echo "-----------------------------分界线-----------------------------" >>"$sparkreport_file"

spark_gettime() {
    local res=$1
    local log_file=$2
    local num_re='^[0-9]+([.][0-9]+)?$'
    if [[ $res == 0 ]]; then
        local RESULT
        RESULT=$(grep "^Time taken:" "$log_file" 2>/dev/null | tail -1 | awk '{print $3}')
        if ! [[ ${RESULT} =~ ${num_re} ]]; then
            echo "Error"
        else
            echo "${RESULT}"
        fi
    else
        echo "Error"
    fi
}

run_one_sql() {
    local file=$1
    local spark_bin=$2
    local log_prefix=$3
    local worker_id=$4
    local worker_report=$5
    local fullname=${file##*/}
    local filename=${fullname%.*}
    local runname=${log_prefix}_w${worker_id}_${filename}
    local spark_res=${omni_home}/logs/spark/${log_prefix}_w${worker_id}_${filename}.result
    local spark_log=${omni_home}/logs/spark/${log_prefix}_w${worker_id}_${filename}.log
    mkdir -p "${omni_home}/logs/spark"
    timeout "$TASK_TIMEOUT" "$spark_bin" "${spark_native[@]}" --database "$DATA_BASE" --name "${spec}_${runname}" -f "$file" 1>"$spark_res" 2>"$spark_log"
    local ret=$?
    local sparkcost_time
    sparkcost_time=$(spark_gettime "$ret" "${spark_log}")
    local row_raw
    row_raw=$(grep '^Time taken: .* seconds, Fetched .* row(s)' "$spark_log" 2>/dev/null | awk '{print $6}' | tail -1)
    local rows
    if [[ -n "${row_raw}" ]] && [[ "${row_raw}" =~ ^[0-9]+$ ]]; then
        rows=$(printf "%8d" "${row_raw}")
    else
        rows="       N/A"
    fi
    local app_id
    app_id=$(grep -o "application_[^[:space:]/]*" "$spark_log" 2>/dev/null | head -n 1 || true)
    local detail_line="Time: $(date "+%Y-%m-%d %T.%3N") ${spark_version} ${DATA_BASE} ${app_id} $runname Fetched_rows: ${rows} cost_seconds:   ${sparkcost_time}"
    printf '%s\n' "$detail_line" >>"$worker_report"
}

worker_run_files() {
    local worker_id=$1
    local spark_bin=$2
    local log_prefix=$3
    local worker_report=$4
    local elapsed_file=$5
    shift 5
    local start_ts
    start_ts=$(date +%s)
    printf '%s\n' "========== worker ${worker_id} start: $(date '+%Y-%m-%d %T') ==========" >>"$worker_report"
    local f
    for f in "$@"; do
        [ -n "$f" ] || continue
        run_one_sql "$f" "$spark_bin" "$log_prefix" "$worker_id" "$worker_report"
    done
    local end_ts
    end_ts=$(date +%s)
    local elapsed=$(( end_ts - start_ts ))
    printf '%s\n' "========== worker ${worker_id} end: $(date '+%Y-%m-%d %T')  total_seconds: ${elapsed} ==========" >>"$worker_report"
    echo "$elapsed" >"$elapsed_file"
}

spark_run_queries() {
    local sql=$1
    local spark_bin=$2
    local log_prefix=${3:-native}
    export spec
    export DATA_BASE
    mkdir -p "${omni_home}/logs/spark"
    mkdir -p "${omni_home}/report"

    shopt -s nullglob
    local files=( "${sqldir}/${sql}"*.sql )
    shopt -u nullglob
    if [ ${#files[@]} -eq 0 ]; then
        echo "spark_run_queries: 未找到匹配 ${sqldir}/${sql}*.sql" >&2
        return 1
    fi

    local sorted_files=()
    while IFS= read -r line; do
        [ -n "$line" ] && sorted_files+=( "$line" )
    done < <(ls -1v "${files[@]}")

    local n=${#sorted_files[@]}
    local c=$SPARK_CONCURRENCY
    local run_ts
    run_ts=$(date +%Y%m%d%H%M%S)
    local tmp_dir="${omni_home}/report/.tmp_${log_prefix}_${run_ts}_$$"
    mkdir -p "$tmp_dir"

    if (( c <= 1 || n == 1 )); then
        local wr="${omni_home}/report/${log_prefix}_w0.report"
        local file
        local start_ts
        start_ts=$(date +%s)
        printf '%s\n' "========== worker 0 start: $(date '+%Y-%m-%d %T') ==========" >>"$wr"
        for file in "${sorted_files[@]}"; do
            run_one_sql "$file" "$spark_bin" "$log_prefix" 0 "$wr"
        done
        local end_ts
        end_ts=$(date +%s)
        local elapsed=$(( end_ts - start_ts ))
        printf '%s\n' "========== worker 0 end: $(date '+%Y-%m-%d %T')  total_seconds: ${elapsed} ==========" >>"$wr"
        local sum_rows
        sum_rows=$(printf "%8d" "${n}")
        append_report_line "Time: $(date "+%Y-%m-%d %T.%3N") ${spark_version} ${DATA_BASE} app_id ${log_prefix}_w0_total Fetched_rows: ${sum_rows} cost_seconds:   ${elapsed}"
        rm -rf "$tmp_dir"
        return 0
    fi

    local k_fwd=$((c / 2))
    local k_rev=$((c - k_fwd))
    if (( k_fwd < 1 )); then
        k_fwd=1
        k_rev=$((c - 1))
    fi

    local mid=$(( (n + 1) / 2 ))
    local fwd_slice=("${sorted_files[@]:0:mid}")
    local rev_slice=("${sorted_files[@]:mid}")
    local rev_ordered=()
    local i
    for ((i = ${#rev_slice[@]} - 1; i >= 0; i--)); do
        rev_ordered+=( "${rev_slice[i]}" )
    done

    local pids=()
    local worker_ids=()
    local worker_sql_counts=()
    local w j base wid

    base=0
    for ((w = 0; w < k_fwd; w++)); do
        wid=$((base + w))
        local batch=()
        for ((j = w; j < ${#fwd_slice[@]}; j += k_fwd)); do
            batch+=( "${fwd_slice[j]}" )
        done
        local wr="${omni_home}/report/${log_prefix}_w${wid}.report"
        local ef="${tmp_dir}/elapsed_w${wid}"
        (worker_run_files "$wid" "$spark_bin" "$log_prefix" "$wr" "$ef" "${batch[@]}") &
        pids+=( "$!" )
        worker_ids+=( "$wid" )
        worker_sql_counts+=( "${#batch[@]}" )
    done
    base=$k_fwd

    for ((w = 0; w < k_rev; w++)); do
        wid=$((base + w))
        local batch=()
        for ((j = w; j < ${#rev_ordered[@]}; j += k_rev)); do
            batch+=( "${rev_ordered[j]}" )
        done
        local wr="${omni_home}/report/${log_prefix}_w${wid}.report"
        local ef="${tmp_dir}/elapsed_w${wid}"
        (worker_run_files "$wid" "$spark_bin" "$log_prefix" "$wr" "$ef" "${batch[@]}") &
        pids+=( "$!" )
        worker_ids+=( "$wid" )
        worker_sql_counts+=( "${#batch[@]}" )
    done

    local pid st=0
    for pid in "${pids[@]}"; do
        if ! wait "$pid"; then
            st=1
        fi
    done

    local idx
    for ((idx = 0; idx < ${#worker_ids[@]}; idx++)); do
        wid=${worker_ids[idx]}
        local cnt=${worker_sql_counts[idx]}
        local ef="${tmp_dir}/elapsed_w${wid}"
        local elapsed="N/A"
        if [ -f "$ef" ]; then
            elapsed=$(cat "$ef")
        fi
        local sum_rows
        sum_rows=$(printf "%8d" "${cnt}")
        append_report_line "Time: $(date "+%Y-%m-%d %T.%3N") ${spark_version} ${DATA_BASE} app_id ${log_prefix}_w${wid}_total Fetched_rows: ${sum_rows} cost_seconds:   ${elapsed}"
    done

    rm -rf "$tmp_dir"
    return "$st"
}

spark_run_native() {
    spark_run_queries "$1" "${SPARK_HOME}/bin/spark-sql" "native"
}

spark_run_omni() {
    local bin="${OMNI_SPARK_SQL:-${SPARK_HOME}/bin/spark-sql}"
    if [[ -z "${OMNI_SPARK_SQL:-}" ]]; then
        echo "spark_run_omni: 未设置 OMNI_SPARK_SQL，使用 \${SPARK_HOME}/bin/spark-sql" >&2
    elif [[ ! -x "$bin" ]]; then
        echo "spark_run_omni: OMNI_SPARK_SQL 不可执行: $bin，改用 \${SPARK_HOME}/bin/spark-sql" >&2
        bin="${SPARK_HOME}/bin/spark-sql"
    fi
    spark_run_queries "$1" "$bin" "omni"
}

run_nmon() {
    local arch
    arch=$(lscpu | grep Architecture | awk '{print $2}')
    if [ X"${arch}" = "Xx86_64" ]; then
        for i in "${server_list[@]}"; do
            ssh "${i}" "ps -ef | grep -v grep | grep nmon |awk '{print \$2}' | xargs kill -9"
            scp -rp "${omni_home}/conf/nmon-x86/nmon" "${i}:~/"
            ssh "${i}" "nohup bash -lc 'cd ~ && chmod +x nmon && ./nmon -f -t -m /root -s 5 -c 1440' >/dev/null 2>&1 &"
        done
    else
        for i in "${server_list[@]}"; do
            ssh "${i}" "ps -ef | grep -v grep | grep nmon |awk '{print \$2}' | xargs kill -9"
            scp -rp "${omni_home}/conf/nmon-arm/nmon" "${i}:~/"
            ssh "${i}" "nohup bash -lc 'cd ~ && chmod +x nmon && ./nmon -f -t -m /root -s 5 -c 1440' >/dev/null 2>&1 &"
        done
    fi
}

close_nmon() {
    if [ -z "${nmon_dir:-}" ]; then
        echo "close_nmon: 缺少 nmon_dir" >&2
        return 1
    fi
    local eff_run="${runname:-batch}"
    local arch
    arch=$(lscpu | grep Architecture | awk '{print $2}')
    for i in "${server_list[@]}"; do
        mkdir -p "${omni_home}/report/${nmon_dir}/${eff_run}"
        scp -rp "${i}:"'~/*.nmon' "${omni_home}/report/${nmon_dir}/${eff_run}/${eff_run}_${i}.nmon"
        if [ X"${arch}" = "Xx86_64" ]; then
            "${omni_home}/conf/nmon-x86/nmonchart" "${omni_home}/report/${nmon_dir}/${eff_run}/${eff_run}_${i}.nmon" "${omni_home}/report/${nmon_dir}/${eff_run}/${eff_run}_${i}.html"
        else
            "${omni_home}/conf/nmon-arm/nmonchart" "${omni_home}/report/${nmon_dir}/${eff_run}/${eff_run}_${i}.nmon" "${omni_home}/report/${nmon_dir}/${eff_run}/${eff_run}_${i}.html"
        fi
        ssh "${i}" "rm ~/*nmon -rf"
        ssh "${i}" "ps -ef | grep -v grep | grep nmon |awk '{print \$2}' | xargs kill -9"
    done
}

clear_cache() {
    for i in "${server_list[@]}"; do
        ssh "$i" "sync;echo 3 > /proc/sys/vm/drop_caches"
    done
}

main() {
    local usage="Usage: bash ${0##*/} [spark|omni|all|nmonclose|-h] [times] [sql_prefix] [spec]  # 并发数见 config.ini: SPARK_CONCURRENCY"
    local RunArgs=${1:-}
    local Times=${2:-}
    local sql=${3:-}
    local spec=${4:-}
    local TIME_CUR
    TIME_CUR=$(date "+%Y-%m-%d-%H-%M-%S-%3N")
    [ -n "${1:-}" ] || RunArgs="all"
    [ -n "${2:-}" ] || Times=1
    [ -n "${3:-}" ] || sql=""
    [ -n "${4:-}" ] || spec='default'
    export spec

    if [ "${RunArgs}" = "spark" ]; then
        local i
        for i in $(seq 1 "$Times"); do
            nmon_dir=nmon_$(date +%Y%m%d%H%M%S)_${spec}
            spark_run_native "$sql"
        done
    elif [ "${RunArgs}" = "nmonclose" ]; then
        close_nmon
    elif [ "${RunArgs}" = "-h" ]; then
        echo "$usage"
    else
        local i
        for i in $(seq 1 "$Times"); do
            nmon_dir=nmon_$(date +%Y%m%d%H%M%S)_${spec}
            run_nmon
            spark_run_omni "$sql"
            mkdir -p "${omni_home}/logs/spark_${TIME_CUR}"
            /bin/cp -rp "${omni_home}/logs/spark/"* "${omni_home}/logs/spark_${TIME_CUR}" 2>&1 &
            close_nmon
        done
    fi
}

main "$@"
