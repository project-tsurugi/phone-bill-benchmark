#!/bin/bash -uex


BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh


# start tsurugidb server

export GLOG_logtostderr=1
$TSURUGI_DIR/bin/tgctl start --conf=$BIN_DIR/etc/phone-bill.ini --v=$TSURUGI_LOG_LEVEL &> $LOG_DIR/tsurugidb-`date "+%Y%m%d_%H%M%S"`.log && true

while [[ ! "`$TSURUGI_DIR/bin/tgctl status --conf=$BIN_DIR/etc/phone-bill.ini`"  == *RUNNING* ]]
do
  sleep 1
done



