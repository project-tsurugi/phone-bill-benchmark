#!/bin/bash -uex


BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh

# kill tsurugidb server

$TSURUGI_DIR/bin/tgctl shutdown --conf=$BIN_DIR/etc/phone-bill.ini && true



