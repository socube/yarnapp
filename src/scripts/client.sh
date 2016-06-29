#!/bin/bash

echo "Running client program"
for i in `seq 1 60`
do
  echo "`date` counter $i"
  sleep 1
done
