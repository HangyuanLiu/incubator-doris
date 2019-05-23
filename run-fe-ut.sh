#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export DORIS_HOME=${ROOT}

. ${DORIS_HOME}/env.sh

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --clean    clean and build ut
     --run    build and run ut

  Eg.
    $0                      build and run ut
    $0 --coverage           build and run coverage statistic
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'coverage' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

COVERAGE=
if [ $# == 1 ] ; then
    #default
    COVERAGE=0
else
    COVERAGE=0
    while true; do 
        case "$1" in
            --coverage) COVERAGE=1 ; shift ;;
            --) shift ;  break ;;
            *) ehco "Internal error" ; exit 1 ;;
        esac
    done
fi

echo "Build Frontend UT"

rm ${DORIS_HOME}/fe/build/ -rf
rm ${DORIS_HOME}/fe/output/ -rf

echo "******************************"
echo "    Runing DorisFe Unittest    "
echo "******************************"

cd ${DORIS_HOME}/fe/
mkdir -p build/compile

if [ ${COVERAGE} -eq 1 ]; then
    echo "Run coverage statistic"
    ant cover-test
else
    echo "Run Frontend UT"
    $MVN test    
fi
