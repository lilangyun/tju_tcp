#!/usr/bin/env bash
# pack_test.sh
# 生成课程报告所需的 tju_tcp 提交包。
#
# 包内结构：
#   tju_tcp/
#   ├── README.md
#   ├── PACKAGE-INFO.txt
#   ├── Makefile
#   ├── inc/
#   ├── src/
#   └── test/
#       ├── Makefile
#       ├── *.c
#       ├── *.py
#       ├── rdt_send_file.txt
#       ├── pack.sh
#       ├── pack_test.sh
#       └── evidence/
#           ├── T-01-delay20ms-loss3pct/
#           ├── perf-20260917-103605/
#           └── newreno-comparison/
#
# 不纳入 docs/、picture/、Word 报告、构建产物和 *.log 运行日志。
#
# 用法：
#   cd /vagrant/tju_tcp/test
#   bash pack_test.sh
#   bash pack_test.sh --outdir /vagrant

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEST_DIR="$REPO_ROOT/test"
T01_EVIDENCE="$TEST_DIR/evidence/T-01-delay20ms-loss3pct"
PERF_EVIDENCE="$TEST_DIR/evidence/perf-20260917-103605"
NR01_EVIDENCE="$TEST_DIR/evidence/newreno-comparison/NR-01-20260921-024944"
OUT_DIR="$REPO_ROOT"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --outdir)
      if [[ $# -lt 2 ]]; then echo "缺少 --outdir 参数" >&2; exit 2; fi
      OUT_DIR="$2"; shift 2 ;;
    -h|--help) sed -n '2,28p' "$0"; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 2 ;;
  esac
done

need_zip() {
  if command -v zip >/dev/null 2>&1; then
    return
  fi
  echo "未找到 zip，尝试安装..."
  sudo apt install zip -y
}

require_path() {
  if [[ ! -e "$1" ]]; then
    echo "缺少必要文件/目录: $1" >&2
    exit 1
  fi
}

mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

PROJECT_DIR="$STAGE/tju_tcp"
PACKAGE_ZIP="$OUT_DIR/tju_tcp_package.zip"

echo "=============================================="
echo "tju_tcp 项目打包"
echo "仓库根目录: $REPO_ROOT"
echo "输出目录  : $OUT_DIR"
echo "T-01 证据  : $T01_EVIDENCE"
echo "perf 证据  : $PERF_EVIDENCE"
echo "NR-01 证据 : $NR01_EVIDENCE"
echo "=============================================="

need_zip
require_path "$REPO_ROOT/README.md"
require_path "$REPO_ROOT/Makefile"
require_path "$REPO_ROOT/inc"
require_path "$REPO_ROOT/src"
require_path "$TEST_DIR/Makefile"
require_path "$TEST_DIR/rdt_send_file.txt"
require_path "$TEST_DIR/pack_test.sh"
require_path "$T01_EVIDENCE"
require_path "$PERF_EVIDENCE"
require_path "$NR01_EVIDENCE"
for pattern in "$TEST_DIR"/*.c "$TEST_DIR"/*.py; do
  require_path "$pattern"
done

# staging：项目顶层
mkdir -p "$PROJECT_DIR"
cp -a "$REPO_ROOT/README.md" "$PROJECT_DIR/"
cp -a "$REPO_ROOT/Makefile" "$PROJECT_DIR/"
cp -a "$REPO_ROOT/inc" "$PROJECT_DIR/"
cp -a "$REPO_ROOT/src" "$PROJECT_DIR/"

# staging：test 目录
mkdir -p "$PROJECT_DIR/test"
cp -a "$TEST_DIR/Makefile" "$PROJECT_DIR/test/"
cp -a "$TEST_DIR"/*.c "$PROJECT_DIR/test/"
cp -a "$TEST_DIR"/*.py "$PROJECT_DIR/test/"
cp -a "$TEST_DIR/rdt_send_file.txt" "$PROJECT_DIR/test/"
cp -a "$TEST_DIR/pack.sh" "$PROJECT_DIR/test/" 2>/dev/null || true
cp -a "$TEST_DIR/pack_test.sh" "$PROJECT_DIR/test/"

# staging：报告证据
# 复制全部非日志 evidence，保留 pcap、Trace、CSV/JSON、配置、图表和源码快照。
mkdir -p "$PROJECT_DIR/test/evidence"
cp -a "$TEST_DIR/evidence/." "$PROJECT_DIR/test/evidence/"
find "$PROJECT_DIR/test/evidence" -type f -name '*.log' -delete

# staging：记录包对应的源码版本，便于报告证据与源码对照。
GIT_BRANCH="$(git -C "$REPO_ROOT" branch --show-current 2>/dev/null || echo unknown)"
GIT_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
GIT_DESCRIBE="$(git -C "$REPO_ROOT" describe --tags --always --dirty 2>/dev/null || echo unknown)"
{
  printf 'package=tju_tcp\n'
  printf 'created_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf 'git_branch=%s\n' "$GIT_BRANCH"
  printf 'git_commit=%s\n' "$GIT_COMMIT"
  printf 'git_describe=%s\n' "$GIT_DESCRIBE"
  printf 'included_evidence=T-01-delay20ms-loss3pct, perf-20260917-103605, newreno-comparison/NR-01-20260921-024944\n'
  printf 'excluded=docs, picture, Word reports, build products, *.log\n'
} > "$PROJECT_DIR/PACKAGE-INFO.txt"

# 生成项目包（zip 根目录为 tju_tcp/）
(
  cd "$STAGE"
  rm -f "$PACKAGE_ZIP"
  zip -r -q "$PACKAGE_ZIP" tju_tcp
)

echo ""
echo "打包完成："
echo "  $PACKAGE_ZIP （$(du -h "$PACKAGE_ZIP" | cut -f1)）"
echo ""
echo "包结构：tju_tcp/{README.md, Makefile, inc/, src/, test/...}"
