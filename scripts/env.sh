#!/bin/bash

if [ -n "${TSURUGI_LOG_LEVEL-}" ]; then
  ORG_TSURUGI_LOG_LEVEL=$TSURUGI_LOG_LEVEL
fi

INSTALL_DIR=$HOME
TSURUGI_DIR=$HOME/tsurugi/tsurugi
TSURUGI_LOG_DIR=$TSURUGI_DIR/var/data/log
TSURUGI_LOG_LEVEL=35
TSURUGI_THREAD_SIZE=80
LOG_DIR=$HOME/logs
ASYNC_PROFILER_DIR=$HOME/async-profiler-2.8.3-linux-x64/

export JAVA_OPTS=-Dcom.tsurugidb.tsubakuro.jniverify=false
export LD_LIBRARY_PATH=$TSURUGI_DIR/lib

if [ -f $HOME/.phonebill ]; then
   . $HOME/.phonebill
fi

if [ -n "${ORG_TSURUGI_LOG_LEVEL-}" ]; then
  TSURUGI_LOG_LEVEL=$ORG_TSURUGI_LOG_LEVEL
fi

sed "s!log_location=.*!log_location=$TSURUGI_LOG_DIR!" $BIN_DIR/etc/phone-bill.ini.template > $BIN_DIR/etc/phone-bill.ini
sed -i s/'$TSURUGI_THREAD_SIZE'/$TSURUGI_THREAD_SIZE/  $BIN_DIR/etc/phone-bill.ini


