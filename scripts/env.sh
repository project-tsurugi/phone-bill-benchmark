#!/bin/bash

INSTALL_DIR=$HOME
TSURUGI_DIR=$HOME/tsurugi/tsurugi
TSURUGI_LOG_DIR=$TSURUGI_DIR/var/data/log
TSURUGI_LOG_LEVEL=30
LOG_DIR=$HOME/logs
ASYNC_PROFILER_DIR=$HOME/async-profiler-2.8.3-linux-x64/

export JAVA_OPTS=-Dcom.tsurugidb.tsubakuro.jniverify=false
export LD_LIBRARY_PATH=$TSURUGI_DIR/lib

if [ -f $HOME/.phonebill ]; then
   . $HOME/.phonebill
fi


