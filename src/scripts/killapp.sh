#!/bin/bash

export CLASSPATH=$CLASSPATH:`hadoop classpath`:../../target/*:.
${JAVA_HOME}/bin/java com.tugo.yarnapp.KillApp $*
