#!/bin/bash -uex


BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh


# start oltp server

export GLOG_logtostderr=1
$TSURUGI_DIR/bin/oltp start --conf=$BIN_DIR/etc/phone-bill.ini --v=30 &> $LOG_DIR/tateyama-server-`date "+%Y%m%d_%H%M%S"`.log && true

while [[ ! "`$TSURUGI_DIR/bin/oltp status --conf=$BIN_DIR/etc/phone-bill.ini`"  == *RUNNING* ]]
do
  sleep 1
done



