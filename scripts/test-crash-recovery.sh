#!/usr/bin/env bash
# Step 5：真机崩溃恢复对账。
# 起 N 节点 → 写若干 KV → kill -9 全部 → 重启 → 校验数据跨崩溃存活。
# 这是唯一让"组合后的 fileWAL"撞上真 fsync + 真 kill -9 的测试（L1 结构上测不了）。
#
# 用法：scripts/test-crash-recovery.sh        （默认 3 节点，端口 6001+）
#       N=5 scripts/test-crash-recovery.sh
set -uo pipefail

N="${N:-3}"
BASE_PORT="${BASE_PORT:-6001}"
HOST=127.0.0.1
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="$REPO_ROOT/src"
RUN_DIR="$REPO_ROOT/.crashtest"
BIN_DIR="$RUN_DIR/bin"

PEERS=""
for ((i=0; i<N; i++)); do
  id="n$((i+1))"; port=$((BASE_PORT + i))
  PEERS+="${PEERS:+,}${id}=${HOST}:${port}"
done

PIDS=()
start_cluster() {
  PIDS=()
  for ((i=0; i<N; i++)); do
    id="n$((i+1))"; port=$((BASE_PORT + i)); data="$RUN_DIR/data/$id"; mkdir -p "$data"
    "$BIN_DIR/raftkvd" --node-id "$id" --listen "${HOST}:${port}" --data-dir "$data" --peers "$PEERS" \
      > "$RUN_DIR/$id.log" 2>&1 &
    PIDS+=($!)
  done
}
kill9_cluster() {
  for pid in "${PIDS[@]:-}"; do kill -9 "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap kill9_cluster EXIT

ctl() { "$BIN_DIR/raftkvctl" --peers "$PEERS" "$@" 2>/dev/null; }

echo "== 构建 =="
( cd "$SRC_DIR" && go build -o "$BIN_DIR/raftkvd" ./cmd/raftkvd && go build -o "$BIN_DIR/raftkvctl" ./cmd/raftkvctl )

echo "== 清空数据（全新集群）=="
rm -rf "$RUN_DIR/data"; mkdir -p "$RUN_DIR/data"

KEYS=(alpha bravo charlie delta)
VALS=(1111 2222 3333 4444)
fail=0

echo "== 启动集群（第一轮）=="
start_cluster
sleep 2 # 给选主一点时间（ctl 自身也会重试找 leader）

echo "== 写入 =="
for ((k=0; k<${#KEYS[@]}; k++)); do
  out="$(ctl put "${KEYS[$k]}" "${VALS[$k]}" 0)"; echo "  put ${KEYS[$k]}=${VALS[$k]} -> ${out:-<无响应>}"
done

echo "== 崩溃前校验 =="
for ((k=0; k<${#KEYS[@]}; k++)); do
  got="$(ctl get "${KEYS[$k]}" | cut -f1)"
  if [[ "$got" == "${VALS[$k]}" ]]; then echo "  ok ${KEYS[$k]}=$got"; else echo "  MISMATCH ${KEYS[$k]}: got '$got' want '${VALS[$k]}'"; fail=1; fi
done

echo "== kill -9 全部节点 =="
kill9_cluster
sleep 1 # 让监听端口释放

echo "== 重启集群（第二轮，同数据目录）=="
start_cluster
sleep 3 # 重新选主 + 重放 fileWAL 中的 log 重建 KV 状态机

echo "== 崩溃+重启后校验（数据必须还在）=="
for ((k=0; k<${#KEYS[@]}; k++)); do
  got="$(ctl get "${KEYS[$k]}" | cut -f1)"
  if [[ "$got" == "${VALS[$k]}" ]]; then echo "  存活 ${KEYS[$k]}=$got"; else echo "  丢失/错配 ${KEYS[$k]}: got '$got' want '${VALS[$k]}'"; fail=1; fi
done

echo
if [[ $fail -eq 0 ]]; then echo "PASS：数据跨 kill -9 全部存活"; else echo "FAIL：有数据丢失/错配"; fi
exit $fail
