git clone -b alloy https://github.com/guillaumerosinosky/prometheus-stack-swarm
cd prometheus-stack-swarm
sed -i 's/node.labels.tier==manager/node.role==manager/g' docker-stack-label.yml
HOSTNAME=$(hostname) docker stack deploy -c docker-stack-label.yml mon