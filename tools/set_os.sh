swapoff -a
sysctl -w vm.swappiness=0
setenforce 0
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag

for i in {a,b,c,d,e,f,g,h}
do

  echo mq-deadline > /sys/block/sd$i/queue/scheduler
  echo 256 > /sys/block/sd$i/queue/nr_requests
  echo 16384 > /sys/block/sd$i/queue/read_ahead_kb
  echo 1024 > /sys/block/sd$i/queue/max_sectors_kb
  echo 32 > /sys/block/sd$i/device/queue_depth
done
