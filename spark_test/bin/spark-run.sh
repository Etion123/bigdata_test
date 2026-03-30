#!/usr/bin/env bash
set -e
# demo-concurrent 仅本地写报告，跳过 /etc/profile，避免 Git Bash / CI 下加载过慢或失败
if [ "${1:-}" = "demo-concurrent" ]; then
    SPARK_RUN_SKIP_PROFILE=1
fi
# profile 内 test 在变量已置位时可能返回 1；先关闭 errexit 再加载
if [ -z "${SPARK_RUN_SKIP_PROFILE:-}" ]; then
    set +e +u
    if [ -f /etc/profile ]; then
        # shellcheck source=/dev/null
        source /etc/profile
    fi
    if [ -f "${HOME}/.bashrc" ]; then
        # shellcheck source=/dev/null
        source "${HOME}/.bashrc"
    fi
    set -e -u
else
    set -e -u
fi

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
if [[ -z "${SQL_EXPLICIT_LIST+x}" ]]; then
    SQL_EXPLICIT_LIST=()
fi

sqldir="${omni_home}/${SQL_SUBDIR}"
sparkreport_file="${omni_home}/${SPARK_REPORT_REL}"
mkdir -p "${omni_home}/report"

if [ -d "${omni_home}/bin" ]; then
    chmod -R +x "${omni_home}/bin" 2>/dev/null || true
fi

REPORT_LOCK="${omni_home}/report/.hibench-spark.report.lock"
append_report_line() {
    local line=$1
    if [ -n "${SPARK_RUN_SKIP_PROFILE:-}" ] || ! command -v flock >/dev/null 2>&1; then
        printf '%s\n' "$line" >>"$sparkreport_file"
        return
    fi
    (
        flock -x 200
        printf '%s\n' "$line" >>"$sparkreport_file"
    ) 200>>"$REPORT_LOCK"
}

# 与 GNU「ls -1v 文件名」一致的自然序：q1→q2→…→q9→q10→…→q14a→q14b→…→q99（对 basename 做sort -V 再还原路径，避免把上百路径当参数传给 ls）
list_sql_files_natural_order_into() {
    local sql_prefix=$1
    local -n _out=$2
    _out=()
    shopt -s nullglob
    local cand=( "${sqldir}/${sql_prefix}"*.sql )
    shopt -u nullglob
    if [ ${#cand[@]} -eq 0 ]; then
        return 1
    fi
    local line
    while IFS= read -r line; do
        [ -n "$line" ] && _out+=( "$line" )
    done < <(
        for f in "${cand[@]}"; do
            printf '%s\t%s\n' "${f##*/}" "$f"
        done | LC_ALL=C sort -t $'\t' -k1,1V | cut -f2-
    )
    return 0
}

# 若 config.ini 中 SQL_EXPLICIT_LIST 非空，只跑列出的查询（不含 .sql），再按自然序排；否则按 sql_prefix 通配
resolve_sorted_sql_files_into() {
    local sql_prefix=$1
    local -n _out=$2
    _out=()
    if [ ${#SQL_EXPLICIT_LIST[@]} -gt 0 ]; then
        local missing=() name path
        local -A _seen
        for name in "${SQL_EXPLICIT_LIST[@]}"; do
            name="${name%.sql}"
            name="${name//[[:space:]]/}"
            [ -z "$name" ] && continue
            path="${sqldir}/${name}.sql"
            if [[ -n "${_seen[$path]:-}" ]]; then
                continue
            fi
            _seen[$path]=1
            if [ ! -f "$path" ]; then
                missing+=( "$path" )
                continue
            fi
            _out+=( "$path" )
        done
        if [ ${#missing[@]} -gt 0 ]; then
            echo "spark-run.sh: SQL_EXPLICIT_LIST 中有不存在的文件:" >&2
            printf '%s\n' "${missing[@]}" >&2
            return 1
        fi
        if [ ${#_out[@]} -eq 0 ]; then
            echo "spark-run.sh: SQL_EXPLICIT_LIST 未解析到任何有效文件" >&2
            return 1
        fi
        local sorted=()
        while IFS= read -r line; do
            [ -n "$line" ] && sorted+=( "$line" )
        done < <(
            for f in "${_out[@]}"; do
                printf '%s\t%s\n' "${f##*/}" "$f"
            done | LC_ALL=C sort -t $'\t' -k1,1V | cut -f2-
        )
        _out=("${sorted[@]}")
        return 0
    fi
    list_sql_files_natural_order_into "$sql_prefix" _out
}

# 与 spark_run_queries 相同的分片与并发结构，仅模拟 SQL 耗时（不调用 spark-sql）
demo_worker_run_files() {
    local worker_id=$1
    local log_prefix=$2
    local worker_report=$3
    local elapsed_file=$4
    shift 4
    local demo_ver="spark-concurrency-demo"
    local sum_sql_seconds=0 sec f fullname filename runname
    printf '%s\n' "========== worker ${worker_id} start: $(date '+%Y-%m-%d %T') ==========" >>"$worker_report"
    for f in "$@"; do
        [ -n "$f" ] || continue
        fullname=${f##*/}
        filename=${fullname%.*}
        runname=${log_prefix}_w${worker_id}_${filename}
        sleep "0.$((RANDOM % 7 + 2))"
        sec="0.$((RANDOM % 8 + 1))"
        sum_sql_seconds=$(awk -v s="${sum_sql_seconds:-0}" -v t="$sec" 'BEGIN { printf "%.3f", s + t }')
        printf '%s\n' "Time: $(date "+%Y-%m-%d %T.%3N") ${demo_ver} ${DATA_BASE} application_demo_${worker_id}_${filename} ${runname} Fetched_rows:       10 cost_seconds:   ${sec}" >>"$worker_report"
    done
    printf '%s\n' "========== worker ${worker_id} end: $(date '+%Y-%m-%d %T')  total_seconds: ${sum_sql_seconds} (sum of per-sql cost) ==========" >>"$worker_report"
    echo "$sum_sql_seconds" >"$elapsed_file"
}

demo_concurrent_queries() {
    local sql=$1
    local log_prefix=${2:-native}
    export spec=${spec:-demo-concurrent}
    mkdir -p "${omni_home}/report"

    local sorted_files=()
    if ! resolve_sorted_sql_files_into "$sql" sorted_files; then
        echo "demo-concurrent: 未解析到 SQL（检查 ${sqldir} 下通配 ${sql}*.sql 或 config.ini 的 SQL_EXPLICIT_LIST）" >&2
        return 1
    fi

    local n=${#sorted_files[@]}
    local c=${SPARK_CONCURRENCY}
    local run_ts
    run_ts=$(date +%Y%m%d%H%M%S)
    local tmp_dir="${omni_home}/report/.tmp_demo_${log_prefix}_${run_ts}_$$"
    mkdir -p "$tmp_dir"

    printf '%s\n' "-----------------------------分界线 (demo-concurrent SPARK_CONCURRENCY=${c} n=${n}) -----------------------------" >>"$sparkreport_file"

    # 仅当并发数<=1 时走单 worker；仅 1 条 SQL 仍要起满 SPARK_CONCURRENCY 个 worker（每人各跑一遍该条）
    if (( c <= 1 )); then
        local wr="${omni_home}/report/${log_prefix}_w0.report"
        local elapsed sum_rows
        demo_worker_run_files 0 "$log_prefix" "$wr" "${tmp_dir}/elapsed_w0" "${sorted_files[@]}"
        elapsed=$(cat "${tmp_dir}/elapsed_w0")
        sum_rows=$(printf "%8d" "${n}")
        append_report_line "Time: $(date "+%Y-%m-%d %T.%3N") spark-concurrency-demo ${DATA_BASE} app_id ${log_prefix}_w0_total Fetched_rows: ${sum_rows} cost_seconds:   ${elapsed}"
        rm -rf "$tmp_dir"
        echo "demo-concurrent: 单 worker 报告 ${wr}（n=${n}, c=${c}）" >&2
        return 0
    fi

    local k_fwd=$((c / 2))
    local k_rev=$((c - k_fwd))
    if (( k_fwd < 1 )); then
        k_fwd=1
        k_rev=$((c - 1))
    fi

    local all_fwd=( "${sorted_files[@]}" )
    local all_rev=()
    local i
    for ((i = n - 1; i >= 0; i--)); do
        all_rev+=( "${sorted_files[i]}" )
    done

    local pids=()
    local worker_ids=()
    local worker_sql_counts=()
    local w base wid ef wr st=0
    local demo_parallel=1
    case "$(uname -s 2>/dev/null)" in
        MINGW* | MSYS* | CYGWIN*) demo_parallel=0 ;;
    esac

    base=0
    for ((w = 0; w < k_fwd; w++)); do
        wid=$((base + w))
        wr="${omni_home}/report/${log_prefix}_w${wid}.report"
        ef="${tmp_dir}/elapsed_w${wid}"
        if (( demo_parallel )); then
            (demo_worker_run_files "$wid" "$log_prefix" "$wr" "$ef" "${all_fwd[@]}") &
            pids+=( "$!" )
        else
            demo_worker_run_files "$wid" "$log_prefix" "$wr" "$ef" "${all_fwd[@]}"
        fi
        worker_ids+=( "$wid" )
        worker_sql_counts+=( "$n" )
    done
    base=$k_fwd

    for ((w = 0; w < k_rev; w++)); do
        wid=$((base + w))
        wr="${omni_home}/report/${log_prefix}_w${wid}.report"
        ef="${tmp_dir}/elapsed_w${wid}"
        if (( demo_parallel )); then
            (demo_worker_run_files "$wid" "$log_prefix" "$wr" "$ef" "${all_rev[@]}") &
            pids+=( "$!" )
        else
            demo_worker_run_files "$wid" "$log_prefix" "$wr" "$ef" "${all_rev[@]}"
        fi
        worker_ids+=( "$wid" )
        worker_sql_counts+=( "$n" )
    done

    local pid
    for pid in "${pids[@]}"; do
        if ! wait "$pid"; then
            st=1
        fi
    done

    local idx wid cnt elapsed sum_rows
    for ((idx = 0; idx < ${#worker_ids[@]}; idx++)); do
        wid=${worker_ids[idx]}
        cnt=${worker_sql_counts[idx]}
        ef="${tmp_dir}/elapsed_w${wid}"
        elapsed="N/A"
        if [ -f "$ef" ]; then
            elapsed=$(cat "$ef")
        fi
        sum_rows=$(printf "%8d" "${cnt}")
        append_report_line "Time: $(date "+%Y-%m-%d %T.%3N") spark-concurrency-demo ${DATA_BASE} app_id ${log_prefix}_w${wid}_total Fetched_rows: ${sum_rows} cost_seconds:   ${elapsed}"
    done

    rm -rf "$tmp_dir"
    if (( demo_parallel )); then
        echo "demo-concurrent: 已写入 ${sparkreport_file} 与 ${omni_home}/report/${log_prefix}_w*.report（${#worker_ids[@]} worker 并行）" >&2
    else
        echo "demo-concurrent: 已写入（MSYS：worker 顺序执行，分片与 Linux 并行版相同） ${sparkreport_file}" >&2
    fi
    return "$st"
}

if [ "${1:-}" = "demo-concurrent" ]; then
    demo_concurrent_queries "${2:-}" "native"
    exit 0
fi

if [ -z "${SPARK_HOME:-}" ]; then
    echo "spark-run.sh: 请设置 SPARK_HOME（环境变量或 config.ini）" >&2
    exit 1
fi
if [ ! -x "${SPARK_HOME}/bin/spark-sql" ]; then
    echo "spark-run.sh: 未找到可执行的 ${SPARK_HOME}/bin/spark-sql" >&2
    exit 1
fi

_spark_home_resolved="$(cd -P "${SPARK_HOME}" 2>/dev/null && pwd || echo "${SPARK_HOME}")"
spark_version="spark-$(basename "${_spark_home_resolved}" | awk -F'-' '{print $2}')"
unset _spark_home_resolved

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
    local cost_acc_name=${6:-}
    local fullname=${file##*/}
    local filename=${fullname%.*}
    local runname=${log_prefix}_w${worker_id}_${filename}
    local spark_res=${omni_home}/logs/spark/${log_prefix}_w${worker_id}_${filename}.result
    local spark_log=${omni_home}/logs/spark/${log_prefix}_w${worker_id}_${filename}.log
    mkdir -p "${omni_home}/logs/spark"
    timeout "$TASK_TIMEOUT" "$spark_bin" "${spark_native[@]}" --database "$DATA_BASE" --name "${spec}_${runname}" -f "$file" 1>"$spark_res" 2>"$spark_log"
    local ret=$?
    local sparkcost_time
    local num_cost='^[0-9]+([.][0-9]+)?$'
    sparkcost_time=$(spark_gettime "$ret" "${spark_log}")
    if [[ -n "$cost_acc_name" ]] && [[ ${sparkcost_time} =~ ${num_cost} ]]; then
        local -n _sql_cost_sum=$cost_acc_name
        _sql_cost_sum=$(awk -v s="${_sql_cost_sum:-0}" -v t="$sparkcost_time" 'BEGIN { printf "%.3f", s + t }')
    fi
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
    local sum_sql_seconds=0
    printf '%s\n' "========== worker ${worker_id} start: $(date '+%Y-%m-%d %T') ==========" >>"$worker_report"
    local f
    for f in "$@"; do
        [ -n "$f" ] || continue
        run_one_sql "$f" "$spark_bin" "$log_prefix" "$worker_id" "$worker_report" sum_sql_seconds
    done
    printf '%s\n' "========== worker ${worker_id} end: $(date '+%Y-%m-%d %T')  total_seconds: ${sum_sql_seconds} (sum of per-sql cost) ==========" >>"$worker_report"
    echo "$sum_sql_seconds" >"$elapsed_file"
}

spark_run_queries() {
    local sql=$1
    local spark_bin=$2
    local log_prefix=${3:-native}
    export spec
    export DATA_BASE
    mkdir -p "${omni_home}/logs/spark"
    mkdir -p "${omni_home}/report"

    local sorted_files=()
    if ! resolve_sorted_sql_files_into "$sql" sorted_files; then
        echo "spark_run_queries: 未解析到 SQL（检查 ${sqldir} 下通配 ${sql}*.sql 或 config.ini 的 SQL_EXPLICIT_LIST）" >&2
        return 1
    fi

    local n=${#sorted_files[@]}
    local c=$SPARK_CONCURRENCY
    local run_ts
    run_ts=$(date +%Y%m%d%H%M%S)
    local tmp_dir="${omni_home}/report/.tmp_${log_prefix}_${run_ts}_$$"
    mkdir -p "$tmp_dir"

    # 仅当并发数<=1 时走单 worker；仅 1 条 SQL 仍要起满 SPARK_CONCURRENCY 个 worker
    if (( c <= 1 )); then
        local wr="${omni_home}/report/${log_prefix}_w0.report"
        local file
        local sum_sql_seconds=0
        printf '%s\n' "========== worker 0 start: $(date '+%Y-%m-%d %T') ==========" >>"$wr"
        for file in "${sorted_files[@]}"; do
            run_one_sql "$file" "$spark_bin" "$log_prefix" 0 "$wr" sum_sql_seconds
        done
        printf '%s\n' "========== worker 0 end: $(date '+%Y-%m-%d %T')  total_seconds: ${sum_sql_seconds} (sum of per-sql cost) ==========" >>"$wr"
        local sum_rows
        sum_rows=$(printf "%8d" "${n}")
        append_report_line "Time: $(date "+%Y-%m-%d %T.%3N") ${spark_version} ${DATA_BASE} app_id ${log_prefix}_w0_total Fetched_rows: ${sum_rows} cost_seconds:   ${sum_sql_seconds}"
        rm -rf "$tmp_dir"
        return 0
    fi

    local k_fwd=$((c / 2))
    local k_rev=$((c - k_fwd))
    if (( k_fwd < 1 )); then
        k_fwd=1
        k_rev=$((c - 1))
    fi

    # 前半数 worker：每人正序跑全量 SQL；后半数 worker：每人反序跑全量 SQL（同序列表各自独立完整跑一遍）
    local all_fwd=( "${sorted_files[@]}" )
    local all_rev=()
    local i
    for ((i = n - 1; i >= 0; i--)); do
        all_rev+=( "${sorted_files[i]}" )
    done

    local pids=()
    local worker_ids=()
    local worker_sql_counts=()
    local w base wid

    base=0
    for ((w = 0; w < k_fwd; w++)); do
        wid=$((base + w))
        local wr="${omni_home}/report/${log_prefix}_w${wid}.report"
        local ef="${tmp_dir}/elapsed_w${wid}"
        (worker_run_files "$wid" "$spark_bin" "$log_prefix" "$wr" "$ef" "${all_fwd[@]}") &
        pids+=( "$!" )
        worker_ids+=( "$wid" )
        worker_sql_counts+=( "$n" )
    done
    base=$k_fwd

    for ((w = 0; w < k_rev; w++)); do
        wid=$((base + w))
        local wr="${omni_home}/report/${log_prefix}_w${wid}.report"
        local ef="${tmp_dir}/elapsed_w${wid}"
        (worker_run_files "$wid" "$spark_bin" "$log_prefix" "$wr" "$ef" "${all_rev[@]}") &
        pids+=( "$!" )
        worker_ids+=( "$wid" )
        worker_sql_counts+=( "$n" )
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
    local usage="Usage: bash ${0##*/} [demo-concurrent [sql_glob_prefix]|spark|omni|all|nmonclose|-h] [times] [sql_prefix] [spec]  # 指定若干查询见 config.ini: SQL_EXPLICIT_LIST"
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
