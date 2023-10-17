#!/bin/bash -uex


BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh

# kill tsurugidb server

$TSURUGI_DIR/bin/tgctl kill --conf=$BIN_DIR/etc/phone-bill.ini -timeout 0 && true
rm -rf  $TSURUGI_LOG_DIR

# start tsurugidb server

export GLOG_logtostderr=1
export GLOG_v=$TSURUGI_LOG_LEVEL
LOGFILE=tsurugidb-`date "+%Y%m%d_%H%M%S"`.log
$TSURUGI_DIR/bin/tgctl start --conf=$BIN_DIR/etc/phone-bill.ini  &> $LOG_DIR/$LOGFILE && true
ln -snf $LOG_DIR/$LOGFILE $LOG_DIR/tsurugidb.log
while [[ ! "`$TSURUGI_DIR/bin/tgctl status --conf=$BIN_DIR/etc/phone-bill.ini`"  == *RUNNING* ]]
do
  sleep 1
done



