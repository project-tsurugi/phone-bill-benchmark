#!/bin/bash

INSTALL_DIR=$HOME
TSURUGI_DIR=$HOME/tsurugi/tsurugi
TSURUGI_LOG_DIR=$TSURUGI_DIR/var/data/log
LOG_DIR=$HOME/logs
ASYNC_PROFILER_DIR=$HOME/async-profiler-2.8.3-linux-x64/

if [ -f $HOME/.phonebill ]; then
   . $HOME/.phonebill
fi
