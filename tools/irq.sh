#!/bin/bash
cpulist=({64..127})
c=0
for ((irq=1081; irq <=1144; irq++))
do
    echo "--irq=$irq"
    tuna --irqs $irq --cpus ${cpulist[c]} --move
    let "c++"
    if [ $c -eq 64 ]; then
        echo "$c"
        #cpulist=({64..127})
        c=0
    else
        echo "$c"
    fi
done
tuna --show_irq | grep 0000:17:00.0
