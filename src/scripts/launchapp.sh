#!/bin/bash

echo "launch script to launching test app"

export CLASSPATH=$CLASSPATH:`hadoop classpath`:../../target/*:.
echo $CLASSPATH
mkdir /tmp/appfiles
cp *.sh /tmp/appfiles
cp ../../target/*.jar /tmp/appfiles
${JAVA_HOME}/bin/java com.tugo.yarnapp.AppLauncher
