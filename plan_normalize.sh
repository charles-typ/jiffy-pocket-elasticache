#!/bin/bash

for i in $(ls *_plan*); do
cat $i | awk '{$1=$1-1520346435; print $0}' > ${i}_norm;
done
