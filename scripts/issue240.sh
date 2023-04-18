#!/bin/bash -uex
set -ue


BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh


# kill oltp server

$TSURUGI_DIR/bin/oltp kill --conf=$BIN_DIR/etc/phone-bill.ini -timeout 0 && true
rm -rf  $TSURUGI_LOG_DIR

# start oltp server

export GLOG_logtostderr=1
$TSURUGI_DIR/bin/oltp start --conf=$BIN_DIR/etc/phone-bill.ini --v=$TSURUGI_LOG_LEVEL &> $LOG_DIR/tateyama-server-`date "+%Y%m%d_%H%M%S"`.log && true

while [[ ! "`$TSURUGI_DIR/bin/oltp status --conf=$BIN_DIR/etc/phone-bill.ini`"  == *RUNNING* ]]
do
  sleep 1
done

# execute phone-bill


ext=`date "+%Y%m%d-%H%M%S"`.log
#export DB_INIT_CMD=$BIN_DIR/tinit.sh
export JAVA_OPTS="$JAVA_OPTS -Dcom.tsurugidb.tsubakuro.jniverify=false \
 -Dlogback.configurationFile=$BIN_DIR/etc/logback.xml \
 -Dlog.summary=$LOG_DIR/summary-$ext \
 -Dlog.detail=$LOG_DIR/detail-$ext"

ln -sf $LOG_DIR/summary-$ext $LOG_DIR/summary.log
ln -sf $LOG_DIR/detail-$ext $LOG_DIR/detail.log

$INSTALL_DIR/phone-bill/bin/run MultipleExecute config-cb-medium/0[23]* config-cb-medium/0[23]* config-cb-medium/0[23]*


PID=`ps -f -u $USER | grep tateyama-server | grep -v grep | awk '{print $2}'`
sleep 10
sudo gdb -p $PID -batch -ex 'call (void)malloc_stats_print(0, 0, 0)'

$TSURUGI_DIR/bin/oltp kill --conf=$BIN_DIR/etc/phone-bill.ini -timeout 0 && true



