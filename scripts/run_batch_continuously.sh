#!/bin/bash -ue
set -ue

if [ $# -ne 1 ]; then
  echo usage: $0 config_file
  exit 1
fi
LABEL=`basename $1 | cut -f 1 -d '.'`
CONF=$1

BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh


# create test data

rm -f $LOG_DIR/$LABEL-client.log
export JAVA_OPTS="$JAVA_OPTS -Dlogback.configurationFile=$BIN_DIR/etc/logback_for_continuously.xml"
$INSTALL_DIR/phone-bill/bin/run CreateTestData $CONF


# execute phone-bill

while true
do
  rm -f $LOG_DIR/$LABEL-client.log
  $INSTALL_DIR/phone-bill/bin/run PhoneBill $CONF
done




