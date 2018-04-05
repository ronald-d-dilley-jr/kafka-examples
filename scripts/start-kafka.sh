cmd="./kafka_2.11-1.1.0/bin/kafka-server-start.sh kafka_2.11-1.1.0/config/server.properties"
echo ${cmd}
${cmd} > /dev/null 2>&1 &
