#!/bin/bash -uex


BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh

# kill oltp server

$TSURUGI_DIR/bin/oltp kill --conf=$BIN_DIR/etc/phone-bill.ini && true



