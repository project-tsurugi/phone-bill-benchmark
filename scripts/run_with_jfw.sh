#!/bin/bash -ue

if [ $# -ne 1 ]; then
  echo usage: $0 config_file
  exit 1
fi
LABEL=`basename $1 | cut -f 1 -d '.'`
CONF=$1

BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh

# start oltp server

sed "s!log_location=.*!log_location=$TSURUGI_LOG_DIR!" $BIN_DIR/etc/phone-bill.ini.template > $BIN_DIR/etc/phone-bill.ini
$TSURUGI_DIR/bin/oltp kill --conf=$BIN_DIR/etc/phone-bill.ini && true
rm -rf  $TSURUGI_LOG_DIR
export GLOG_logtostderr=1

$TSURUGI_DIR/bin/oltp start --conf=etc/phone-bill.ini --v=30 &> $LOG_DIR/$LABEL-tateyama-server.log && true

# create test data

rm -f $LOG_DIR/$LABEL-client.log
export JAVA_OPTS="$JAVA_OPTS -Dlogback.configurationFile=etc/logback.xml -Dlogfile=$LOG_DIR/$LABEL-client.log"
$INSTALL_DIR/phone-bill/bin/run CreateTestData $CONF


# execute phone-bill batch with profiling


export JAVA_OPTS="$JAVA_OPTS -XX:StartFlightRecording=name=on_startup,\
filename=$LOG_DIR/$LABEL.jfr,\
dumponexit=true,\
settings=$BIN_DIR/etc/myprofile"

$INSTALL_DIR/phone-bill/bin/run PhoneBill $CONF


# shutdown oltp server

$TSURUGI_DIR/bin/oltp kill --conf=$BIN_DIR/etc/phone-bill.ini && true


