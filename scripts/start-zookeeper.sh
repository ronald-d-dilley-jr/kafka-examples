cmd="./kafka_2.11-1.1.0/bin/zookeeper-server-start.sh kafka_2.11-1.1.0/config/zookeeper.properties"
echo ${cmd}
${cmd} > /dev/null 2>&1 &
