#/bin/bash
if [ "$HOSTNAME" = tools ]; then
  echo "We don't need to update hosts in the tools container. Exiting."
  exit 1
fi

cat << EOF >> /etc/hosts
# some entries for docker containers used in DEV
127.0.0.1 kafka
127.0.0.1 zookeeper
127.0.0.1 schema-registry
127.0.0.1 connect
127.0.0.1 ksql-server
127.0.0.1 ksqldb-server
127.0.0.1 postgres
EOF
echo Done!
