#!/bin/bash -e
#
# Copyright 2023-2024 Project Tsurugi.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ $# -lt 1 ]; then
  echo usage: $0 config_file [output_dir]
  exit 1
fi

BIN_DIR=$(cd $(dirname $0); pwd)
OUTPUT_DIR="${2:-dump}"

ext=`date "+%Y%m%d-%H%M%S"`.log
export JAVA_OPTS="$JAVA_OPTS -Dcom.tsurugidb.tsubakuro.jniverify=false \
 -Dphone-bill.parquet.output.dir=$OUTPUT_DIR \
 -Dlogback.configurationFile=$BIN_DIR/../etc/logback.xml \
 -Dlogfile=$BIN_DIR/../logs/create_test_data_parquet-$ext"

$BIN_DIR/run CreateTestDataParquet "$1"
