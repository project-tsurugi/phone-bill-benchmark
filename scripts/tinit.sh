#!/bin/bash -uex


BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh

# kill oltp server

$TSURUGI_DIR/bin/oltp kill --conf=$BIN_DIR/etc/phone-bill.ini -timeout 0 && true
rm -rf  $TSURUGI_LOG_DIR

# start oltp server

export GLOG_logtostderr=1
LOGFILE=tateyama-server-`date "+%Y%m%d_%H%M%S"`.log
$TSURUGI_DIR/bin/oltp start --conf=$BIN_DIR/etc/phone-bill.ini --v=$TSURUGI_LOG_LEVEL &> $LOG_DIR/$LOGFILE && true
ln -snf $LOG_DIR/$LOGFILE $LOG_DIR/tateyama-server.log
while [[ ! "`$TSURUGI_DIR/bin/oltp status --conf=$BIN_DIR/etc/phone-bill.ini`"  == *RUNNING* ]]
do
  sleep 1
done



