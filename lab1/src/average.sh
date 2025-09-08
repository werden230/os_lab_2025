#!/bin/bash

count=$#
sum=0

for number in "$@"; do
        sum=$(($sum + $number))
done

average=$(($sum / $count))

echo "Count: $count"
echo "Sum: $sum"
echo "Average: $average"
