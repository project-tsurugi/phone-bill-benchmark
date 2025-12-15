#!/bin/bash -ue
set -ue

if [ $# -eq 0 ]; then
  echo usage: $0 config_files
  exit 1
fi

BIN_DIR=$(cd $(dirname $0); cd ..; pwd)

. $BIN_DIR/env.sh


# execute phone-bill


ext=`date "+%Y%m%d-%H%M%S"`.log
export JAVA_OPTS="$JAVA_OPTS -Dcom.tsurugidb.tsubakuro.jniverify=false \
 -Dlogback.configurationFile=$BIN_DIR/etc/logback.xml \
 -Dlog.summary=$LOG_DIR/summary-$ext \
 -Dlog.detail=$LOG_DIR/detail-$ext \
 -Dlog.detail.online=$LOG_DIR/detail-online-$ext"

ln -sf summary-$ext $LOG_DIR/summary.log
ln -sf detail-$ext $LOG_DIR/detail.log
ln -sf detail-online-$ext $LOG_DIR/detail-online.log

# ---- reset latency log ----
: > ~/logs/tsurugidb.log

# ---- measure and display nicely formatted elapsed time ----
TIME_LOG=$LOG_DIR/time-$ext.log

/usr/bin/time -f "Elapsed time: %E (user %Us, sys %Ss)" \
  -o "$TIME_LOG" \
  "$INSTALL_DIR/phone-bill/bin/run" MultipleExecute "$1" \
  | python3 monitor_progress_wl.py --latency-log ~/logs/tsurugidb.log --exit-on-done

cat $TIME_LOG
