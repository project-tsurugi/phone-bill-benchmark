#!/bin/bash -ue

if [ $# -ne 1 ]; then
  echo usage: $0 config_file
  exit 1
fi
LABEL=`basename $1 | cut -f 1 -d '.'`
CONF=$1

BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh

# start tsurugidb server

$TSURUGI_DIR/bin/tgctl kill --conf=$BIN_DIR/etc/phone-bill.ini && true
rm -rf  $TSURUGI_LOG_DIR
export GLOG_logtostderr=1

$TSURUGI_DIR/bin/tgctl start --conf=etc/phone-bill.ini --v=$TSURUGI_LOG_LEVEL &> $LOG_DIR/$LABEL-tsurugidb.log && true
PID_SERVER=`ps -f -u $USER | grep tsurugidb | grep -v grep | awk '{print $2}'`

# create test data

rm -f $LOG_DIR/$LABEL-client.log
export JAVA_OPTS="$JAVA_OPTS -Dlogback.configurationFile=$BIN_DIR/etc/logback.xml -Dlogfile=$LOG_DIR/$LABEL-client.log"
$INSTALL_DIR/phone-bill/bin/run CreateTestData $CONF


# execute phone-bill batch with profiling

#/usr/bin/perf record -a --call-graph dwarf -p $PID_SERVER &
/usr/bin/perf record -a -g -p $PID_SERVER &

export JAVA_OPTS="$JAVA_OPTS -agentpath:$ASYNC_PROFILER_DIR/build/libasyncProfiler.so=start,event=wall,include="'com/tsurugidb/*'",threads,file=$LOG_DIR/$LABEL-client-fg.html"
$INSTALL_DIR/phone-bill/bin/run PhoneBill $CONF

kill %1
wait

# shutdown tsurugidb server

$TSURUGI_DIR/bin/tgctl kill --conf=$BIN_DIR/etc/phone-bill.ini && true

# create flame graph from tsurugidb

/usr/bin/perf script -i perf.data | stackcollapse-perf.pl | flamegraph.pl > $LOG_DIR/$LABEL-server-fg.svg

# Clean up

rm -f perf.data*

