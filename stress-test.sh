#!/usr/bin/env bash
# Usage: ./stress-test.sh <dir> <test_regex> <runs>
# Example: ./stress-test.sh raft1 3A 50

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <package_dir> <test_pattern> [runs]"
    echo "Example: $0 raft1 3A 20"
    echo "Example: $0 kvsrv1 . 10"
    exit 1
fi

DIR=$1
TEST_NAME=$2
COUNT=${3:-50}

if [ ! -d "src/$DIR" ]; then
    echo "Error: Directory src/$DIR does not exist."
    exit 1
fi

cd "src/$DIR" || exit 1

echo "=========================================================="
echo ">>> 开始对 src/$DIR 中的 '$TEST_NAME' 进行 $COUNT 次回归测试..."
echo "=========================================================="
FAIL=0

for i in $(seq 1 "$COUNT"); do
    printf "Run %3d/%-3d: " "$i" "$COUNT"
    OUT=$(go test -race -run "$TEST_NAME" -timeout 5m 2>&1)
    if [ $? -eq 0 ]; then
        echo "PASS"
    else
        echo "FAIL ❌"
        FAIL=$((FAIL + 1))
        LOGFILE="fail_${DIR}_${TEST_NAME}_${i}.log"
        echo "$OUT" > "$LOGFILE"
        echo "   [!] 失败日志已记录到: src/$DIR/$LOGFILE"
    fi
done

echo "=========================================================="
echo "测试总结: 共运行 $COUNT 次，成功 $((COUNT - FAIL)) 次，失败 $FAIL 次"
FAIL_RATE=$(awk "BEGIN {printf \"%.2f\", ($FAIL / $COUNT) * 100}")
echo "失败率: ${FAIL_RATE}%"
echo "=========================================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
