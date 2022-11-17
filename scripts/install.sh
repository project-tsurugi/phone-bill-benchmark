#!/bin/bash -xeu


cd `dirname $0`
source env.sh

cd ../src
./gradlew clean distTar
if [ ! -d $INSTALL_DIR ]; then
  mkdir $INSTALL_DIR
fi
tar x -C $INSTALL_DIR -f build/distributions/phone-bill.tar
