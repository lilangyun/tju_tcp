#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TRACE_DIR="${SCRIPT_DIR}/simulated"

cd "${PROJECT_DIR}"

python3 test/simulate_trace.py --output-dir test/simulated

for name in normal fast_retransmit rto_timeout rtt_rto zero_window close; do
    for actor in client server; do
        python3 test/gen_graph_win.py \
            "test/simulated/${name}_${actor}.event.trace"
    done
done

echo
echo "模拟 trace：${TRACE_DIR}"
echo "图像输出：/vagrant/tju_tcp/test"
