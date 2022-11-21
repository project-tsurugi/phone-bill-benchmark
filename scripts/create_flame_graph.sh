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

$TSURUGI_DIR/bin/oltp kill --conf=$BIN_DIR/etc/phone-bill.ini && true
rm -rf  $TSURUGI_DIR/var/data/log
export GLOG_logtostderr=1

$TSURUGI_DIR/bin/oltp start --conf=etc/phone-bill.ini --v=30 &> $LOG_DIR/$LABEL-tateyama-server.log && true
PID_SERVER=`ps -ef | grep tateyama-server | grep -v grep | awk '{print $2}'`

# create test data

rm -f $LOG_DIR/$LABEL-client.log
export JAVA_OPTS="$JAVA_OPTS -Dlogback.configurationFile=etc/logback.xml -Dlogfile=$LOG_DIR/$LABEL-client.log"
$INSTALL_DIR/phone-bill/bin/run CreateTestData $CONF


# execute phone-bill batch with profiling

#/usr/bin/perf record -a --call-graph dwarf -p $PID_SERVER &
/usr/bin/perf record -a -g -p $PID_SERVER &

export JAVA_OPTS="$JAVA_OPTS -agentpath:$ASYNC_PROFILER_DIR/build/libasyncProfiler.so=start,event=wall,include="'com/tsurugidb/*'",threads,file=$LOG_DIR/$LABEL-client-fg.html"
$INSTALL_DIR/phone-bill/bin/run PhoneBill $CONF

kill %1
wait

# create flame graph from tateyama-server

/usr/bin/perf script | stackcollapse-perf.pl | flamegraph.pl > $LOG_DIR/$LABEL-server-fg.svg


