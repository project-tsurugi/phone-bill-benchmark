#!/bin/bash -uex


BIN_DIR=$(cd $(dirname $0); pwd)

. $BIN_DIR/env.sh

# kill oltp server

sed "s!log_location=.*!log_location=$TSURUGI_LOG_DIR!" $BIN_DIR/etc/phone-bill.ini.template > $BIN_DIR/etc/phone-bill.ini
$TSURUGI_DIR/bin/oltp shutdown --conf=$BIN_DIR/etc/phone-bill.ini && true



