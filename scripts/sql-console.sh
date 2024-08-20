#!/bin/bash -ue


BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh


# start tsurugidb server

$TSURUGI_DIR/bin/tgsql -c ipc:phone-bill



