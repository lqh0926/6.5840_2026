#!/usr/bin/env bash
# 本地多进程起一个 raftkvd 集群（Phase 1 临时脚手架；Phase 3 升级成 docker-compose）。
#
# 用法：
#   scripts/run-local-cluster.sh            # 起 3 节点（默认），数据保留、可恢复
#   N=5 scripts/run-local-cluster.sh        # 起 5 节点
#   scripts/run-local-cluster.sh --clean    # 先清空数据再起（全新集群）
#
# 起好后另开终端：
#   .cluster/bin/raftkvctl --peers "<脚本打印的 peers>" put hello world 0
#   .cluster/bin/raftkvctl --peers "<...>" get hello
# Ctrl-C 停止整个集群（数据保留在 .cluster/data，下次重启自动从落盘恢复）。
set -euo pipefail

N="${N:-3}"
BASE_PORT="${BASE_PORT:-5001}"
HOST=127.0.0.1

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="$REPO_ROOT/src"
RUN_DIR="$REPO_ROOT/.cluster"
BIN_DIR="$RUN_DIR/bin"

if [[ "${1:-}" == "--clean" ]]; then
  rm -rf "$RUN_DIR/data" "$RUN_DIR"/*.log 2>/dev/null || true
  echo "已清空集群数据"
fi
mkdir -p "$BIN_DIR"

echo "构建 raftkvd / raftkvctl ..."
( cd "$SRC_DIR" && go build -o "$BIN_DIR/raftkvd" ./cmd/raftkvd && go build -o "$BIN_DIR/raftkvctl" ./cmd/raftkvctl )

# 组装 peers 串：n1=host:port,n2=...
PEERS=""
for ((i=0; i<N; i++)); do
  id="n$((i+1))"; port=$((BASE_PORT + i))
  PEERS+="${PEERS:+,}${id}=${HOST}:${port}"
done

PIDS=()
cleanup() {
  echo; echo "停止集群 ..."
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

for ((i=0; i<N; i++)); do
  id="n$((i+1))"; port=$((BASE_PORT + i))
  data="$RUN_DIR/data/$id"; mkdir -p "$data"
  "$BIN_DIR/raftkvd" --node-id "$id" --listen "${HOST}:${port}" --data-dir "$data" --peers "$PEERS" \
    > "$RUN_DIR/$id.log" 2>&1 &
  PIDS+=($!)
  echo "启动 $id → ${HOST}:${port}  (log: .cluster/$id.log)"
done

cat <<EOF

集群已启动（$N 节点，peers 如下）。另开终端试：
  peers="$PEERS"
  $BIN_DIR/raftkvctl --peers "\$peers" put hello world 0
  $BIN_DIR/raftkvctl --peers "\$peers" get hello

看日志: tail -f $RUN_DIR/n*.log
Ctrl-C 停止（数据保留在 .cluster/data，下次自动恢复；--clean 清空）。
EOF

wait
