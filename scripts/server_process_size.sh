#!/bin/bash -ue


BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh


# start oltp server

#$TSURUGI_DIR/bin/oltp start --conf=$BIN_DIR/etc/phone-bill.ini --v=30 &> 

LOGFILE=$LOG_DIR/ps-`date "+%Y%m%d_%H%M%S"`.log

echo `date "+%H:%M:%S"`":  "`ps -o pid,vsz,rss,cmd   -p \`$TSURUGI_DIR/bin/oltp pid --conf=$BIN_DIR/etc/phone-bill.ini\` | head -n 1` | tee -a  $LOGFILE
while true
do
  echo `date "+%H:%M:%S"`":  "`ps -o pid,vsz,rss,cmd -h -p \`$TSURUGI_DIR/bin/oltp pid --conf=$BIN_DIR/etc/phone-bill.ini\`` | tee -a $LOGFILE
  sleep 1
done



