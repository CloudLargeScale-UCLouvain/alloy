import enoslib as en
import logging
import re
import time
import requests
import os
import datetime
import pandas as pd
import yaml
from copy import deepcopy

# Use the large /tmp partition (sda5, 375G) for Ansible remote temp files
# instead of /root/.ansible/tmp on the small root partition (sda2, 29G).
os.environ.setdefault('ANSIBLE_REMOTE_TMP', '/tmp/.ansible/tmp')

en.init_logging(level=logging.INFO)
en.check()

class AlloyG5KExperimentation:
    roles = None
    pattern_manager = None
    def __init__(self, roles, pattern_manager):
        self.roles = roles
        self.pattern_manager = pattern_manager

    # Function to extract service name

    def extract_service_name_cpu(column_name):
        match = re.search(r'="[^_]+_(.+)"', column_name)
        # Regular expression to capture up to the second period
        pattern = r'^([^.]+\.[^.]+)\.'

        # Search for the pattern in the column name
        match = re.search(pattern, match.group(1))
        return "cpu" + "_" + match.group(1) if match else column_name
        
    def extract_service_name_network(column_name):
        match = re.search(r'="[^_]+_(.+)"', column_name)
        # Regular expression to capture up to the second period
        pattern = r'^([^.]+\.[^.]+)\.'

        # Search for the pattern in the column name
        match = re.search(pattern, match.group(1))
        return "network" + "_" + match.group(1) if match else column_name

    def extract_service_name_tput(column_name):
        match = re.search(r'="[^_]+_(.+)"', column_name)
        # Regular expression to capture up to the second period
        pattern = r'^([^.]+\.[^.]+)\.'

        # Search for the pattern in the column name
        match = re.search(pattern, match.group(1))
        return "tput" + "_" + match.group(1) if match else column_name

    # Install docker
    def init_docker_swarm(self):
        registry_opts = dict(type="external", ip="docker-cache.grid5000.fr", port=80)
        d = en.Docker(
            agent=self.roles["control"], bind_var_docker="/tmp/docker", registry_opts=registry_opts,
            swarm=True,
            #docker_version="26.1.3",
            # Optional credentials for docker hub
            # credentials=dict(login="mylogin", password="mytoken"
        )

        d.deploy()

        # Redirect Docker and containerd to the large /tmp partition (sda5, 375G).
        # bind_var_docker sets up a bind mount for Docker but containerd stores image
        # content separately in /var/lib/containerd and also needs to be redirected.
        with en.play_on(roles=self.roles, pattern_hosts="control") as p:
            # Free space on the root partition before writing config files to it.
            p.shell('apt-get clean && journalctl --vacuum-size=50M')
            p.shell(
                'mkdir -p /tmp/docker /tmp/containerd && '
                'echo \'{"data-root": "/tmp/docker"}\' > /etc/docker/daemon.json && '
                'mkdir -p /etc/containerd && '
                'containerd config default | sed \'s|/var/lib/containerd|/tmp/containerd|g\' > /etc/containerd/config.toml && '
                'systemctl restart containerd && '
                'systemctl restart docker'
            )

    def load_local_images(self, images_dir):
        tarballs = ["alloy-envoy.tar.gz", "alloy-control-plane.tar.gz"]
        with en.play_on(roles=self.roles, pattern_hosts="control") as p:
            for tarball in tarballs:
                p.copy(src=f"{images_dir}/{tarball}", dest=f"/tmp/{tarball}")
                p.raw(f"docker load < /tmp/{tarball}")

    def init_docker_nodes(self):
        with en.play_on(roles=self.roles, pattern_hosts=self.pattern_manager) as p:
            for name, nodes in self.roles.items():
                for node in nodes:
                    p.raw(f"docker node update --label-add tier={name} {node.alias}")

    def init_docker_network(self):
        with en.play_on(roles=self.roles, pattern_hosts="control[0]") as p:
            p.docker_network(
                name="alloy-test_default",
                attachable=True,
                driver="overlay",
            )

    def init_monitoring(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with en.play_on(roles=self.roles, pattern_hosts=self.pattern_manager) as t:
            t.copy(src=dir_path+"/deploy-monitoring.sh", dest="./", mode="0755")
            t.shell("./deploy-monitoring.sh")

    def generate_config_files(self, compose_file, envoy_file, parallelism, output_file_name="compose.yml"):
        if envoy_file is not None: 
            with open(envoy_file) as f:
                config = yaml.safe_load(f)
            for i in range(1, parallelism+1):
                new_config = deepcopy(config)
                
                # NODE
                node = new_config["node"]["cluster"].replace("edge1", f"edge{i}")
                new_config["node"]["cluster"] = node
                new_config["node"]["id"] = node
        
                # CLUSTERS
                endpoint = new_config["static_resources"]["clusters"][0]["load_assignment"]["endpoints"][0]["lb_endpoints"][0]["endpoint"]["address"]["socket_address"]["address"]
                new_config["static_resources"]["clusters"][0]["load_assignment"]["endpoints"][0]["lb_endpoints"][0]["endpoint"]["address"]["socket_address"]["address"] = endpoint.replace("edge1", f"edge{i}")
                #PARALLELISM
                alloy_config = new_config["static_resources"]["listeners"][0]["filter_chains"][0]["filters"][0]["typed_config"]["config"]["configuration"]["value"]
                new_config["static_resources"]["listeners"][0]["filter_chains"][0]["filters"][0]["typed_config"]["config"]["configuration"]["value"] = alloy_config.replace("num_sources: 1", f"num_sources: {parallelism}").replace("num_partitions: 1", f"num_partitions: {parallelism}")
        
                with open(f"envoy{i}.yml", "w+") as f:
                    yaml.dump(new_config, f)

        if compose_file is not None: 
            with open(compose_file) as f:
                compose = yaml.safe_load(f)
            for i in range(2, parallelism+1):
                # ENVOY
                if "envoy1" in compose["services"]:
                    envoy = deepcopy(compose["services"]["envoy1"])
                    envoy_config = envoy["configs"][0]["source"]
                    envoy["configs"][0]["source"] = envoy_config.replace("edge1", f"edge{i}")
                    placement = envoy["deploy"]["placement"]["constraints"][0]
                    envoy["deploy"]["placement"]["constraints"][0] = placement.replace("kafka1", f"kafka{i}")
                    compose["services"][f"envoy{i}"] = envoy
        
                #KAFKA
                if "kafka-edge1" in compose["services"]:
                    kafka = deepcopy(compose["services"]["kafka-edge1"])
                    kafka["environment"]["KAFKA_BROKER_ID"] = i
                    listeners = kafka["environment"]["KAFKA_ADVERTISED_LISTENERS"]
                    listeners = listeners.replace("envoy1", f"envoy{i}").replace("9095", f"{9195+i}")
                    kafka["environment"]["KAFKA_ADVERTISED_LISTENERS"] = listeners
                    placement = kafka["deploy"]["placement"]["constraints"][0]
                    kafka["deploy"]["placement"]["constraints"][0] = placement.replace("kafka1", f"kafka{i}")
                    compose["services"][f"kafka-edge{i}"] = kafka
        
                # CONFIG
                if "envoy1" in compose["services"]:
                    config = deepcopy(compose["configs"]["envoy-edge1"])
                    config["file"] = config["file"].replace("test1", f"test{i}")
                    compose["configs"][f"envoy-edge{i}"] = config
            with open(output_file_name, "w+") as f:
                    yaml.dump(compose, f)

    def deploy_swarm(self, compose_file="docker-compose.yml", name="test", env={}, wait=30, remove=True):
        print(f"Cleaning old swarm deployment {name}")

        with en.play_on(roles=self.roles, pattern_hosts=self.pattern_manager) as t:
            if remove:
                # Remove the old swarm stack
                t.shell(cmd=f"docker stack rm {name} || true")
            # Copy the entire overhead directory from the control machine to the target machine
            t.copy(src=f"{compose_file}", dest="./")
            # Deploy the new swarm stack
            env_str = " ".join("{}='{}'".format(*i) for i in env.items())
            command = f"{env_str} docker stack deploy --compose-file ./{os.path.basename(compose_file)} {name}"
            print(f"Launching new swarm deployment {name}: {command}")
            t.shell(cmd=command)
        if wait:
            print(f"Wait for {wait} seconds")
            time.sleep(wait)     
            
    PROMETHEUS_PORT = 9090

    def get_metrics_tunnel(self, min_date, max_date, metric_query="container_cpu_usage_seconds_total", step="5s", metric="cpu"):
        with en.G5kTunnel(self.roles["control"][0].address, self.PROMETHEUS_PORT) as (local_address, local_port, _):
            from prometheus_pandas import query
            p = query.Prometheus(f"http://admin:prom-operator@{local_address}:{local_port}")    
            
            min_date_param = min_date.replace(microsecond=0).isoformat()+"Z"
            max_date_param = max_date.replace(microsecond=0).isoformat()+"Z"
            print (min_date_param + "-" + max_date_param)
            df_metric = p.query_range(
                metric_query,
                min_date.replace(microsecond=0).isoformat(),
                max_date.replace(microsecond=0).isoformat(),
                step
            )
            #df_metric = df_metric.rename(columns={df_metric.columns[0]: "ts"})
            # Rename columns
            if metric == "cpu":
                df_metric = df_metric.rename(columns=AlloyG5KExperimentation.extract_service_name_cpu)
            elif metric == "network":
                df_metric = df_metric.rename(columns=AlloyG5KExperimentation.extract_service_name_network)
        return df_metric

    def launch_berserker_publisher(self, host="kafka-edge1", compose_file="./docker-compose.berserker.yml", port="9092", topic="source", target_rps=1, replicas=1, key_name="id", key_value="identifier"):
        
        with en.play_on(roles=self.roles, pattern_hosts=self.pattern_manager) as t:
            # Remove the docker service
            t.shell(cmd="docker service rm test_berserker || true")
            t.copy(src=f"../../berserker", dest="./")
            t.copy(src=f"../../docker-compose.berserker.yml", dest="./")
            # Deploy the docker stack
            line = f"HOST={host} PORT={port} THROUGHPUT={target_rps} TOPIC={topic} REPLICAS={replicas} KEY_NAME={key_name} KEY_VALUE={key_value} docker stack deploy --compose-file {compose_file} test"
            print(line)
            t.shell(cmd=line)      

    def stop_publisher(self):
        with en.play_on(roles=self.roles, pattern_hosts=self.pattern_manager) as t:
            t.shell(cmd="docker service rm test_berserker || true")




    import os 

    def deploy_job(self, filename, class_name, args=None, port=8081):
        with en.G5kTunnel(self.roles["control"][0].address, port) as (local_address, local_port, _):
            print(f"{local_address}:{local_port}")
            res = requests.post(f"http://{local_address}:{local_port}/jars/upload", 
                                #headers={"Content-Type":},
                                #data={},
                                files={"jarfile": (
                                    os.path.basename(filename),
                                    open(filename, "rb"),
                                    "application/x-java-archive"
                                )})
            
            jar_id = str(res.json()["filename"])
            jar_id = jar_id.split("/")[-1]
            print(jar_id)
            params = {
                "entry-class": class_name
            }
            if args is not None:
                comma_separated_string = ','.join([f'{k},{v}' for k, v in args.items()])
                params["programArg"] = comma_separated_string
            res = requests.post(f"http://{local_address}:{local_port}/jars/{jar_id}/run", 
                                    params=params,
                                )
            print(res.json())
            return(res.json()["jobid"])

    def deploy_local_job(self, filename, class_name, args=None, port=8081):
        local_address = "127.0.0.1"
        local_port = port
        print(f"{local_address}:{local_port}")
        res = requests.post(f"http://{local_address}:{local_port}/jars/upload", 
                            #headers={"Content-Type":},
                            #data={},
                            files={"jarfile": (
                                os.path.basename(filename),
                                open(filename, "rb"),
                                "application/x-java-archive"
                            )})
        
        jar_id = str(res.json()["filename"])
        jar_id = jar_id.split("/")[-1]
        print(jar_id)
        params = {
            "entry-class": class_name
        }
        if args is not None:
            comma_separated_string = ','.join([f'{k},{v}' for k, v in args.items()])
            params["programArg"] = comma_separated_string
        res = requests.post(f"http://{local_address}:{local_port}/jars/{jar_id}/run", 
                                params=params,
                            )
        print(res.json())
        return(res.json()["jobid"])
        
    def stop_job(self, job_id):
        with en.G5kTunnel(self.roles["control"][0].address, 8081) as (local_address, local_port, _):
            requests.patch(f"http://{local_address}:{local_port}/jobs/{job_id}")
                
    ######################
    def deploy_stack(self, compose_file="config/docker-compose-ds2.yml", envoy_config="config/envoy-alloy-swarm.yml", env={}, remove=True, copy_envoy=True):
        self.generate_config_files(compose_file, envoy_config, env.get("PARALLELISM", 1))
        if copy_envoy:
            with en.play_on(roles=self.roles, pattern_hosts=self.pattern_manager) as t:
                for i in range(env.get("PARALLELISM", 1)):
                    t.copy(src=f"envoy{i+1}.yml", dest="./")
                    env[f"ENVOY{i+1}_CONFIG"] = f"envoy{i+1}.yml"

        self.deploy_swarm(compose_file="compose.yml", env=env, remove=remove)
        
    def deploy_stack_and_publish(self, compose_file, target_rps, nb_sources=2, nb_partitions=2, nb_tm=3, nb_ts=1, nb_cpu=2, filters_data="", envoy_version="v1.29-alloy-nolog", partition_criteria="label", projection_criteria="", debug=False):
        env = {
            "NUM_SOURCES": nb_sources,
            "NUM_PARTITIONS": nb_partitions,
            "NUM_TM": nb_tm,
            "NUM_TS": nb_ts,
            "NUM_CPU": nb_ts,
            "FILTERS_DATA": filters_data,
            "ENVOY_VERSION": envoy_version,
            "PARTITION_CRITERIA": partition_criteria,
            "PROJECTION_CRITERIA": projection_criteria, 
        }
        if debug:
            env["DEBUG"] = "rest.flamegraph.enabled: true"
        self.deploy_stack(compose_file, env)

    def launch_run(self, job_name=None, jar_name="../../target/alloy-flink-tests-0.1.jar", class_name="TestGroupBy", job_params={}, duration=120):
        now_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S') 
        if "--jobName" not in job_params:
            job_name = f"undefined-{class_name}"
        else:
            job_name = job_params["--jobName"]
        job_name = f"{job_name}_{now_str}"
        job_id = self.deploy_job(jar_name, f"{class_name}", job_params)
        
        start = datetime.datetime.now(tz=datetime.timezone.utc)
        print(f"Sleeping for {duration}s")
        time.sleep(duration)
        self.stop_job(job_id)
        end =  datetime.datetime.now(tz=datetime.timezone.utc)
        cpu_usage = self.get_metrics_tunnel(start, end, metric_query='sum(irate(container_cpu_usage_seconds_total{image!="", name=~"test_task.*|alloy_envoy.*|test_kafka.*"}[1m])) by (name)')
        tput = self.get_metrics_tunnel(start, end, metric_query='sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate)', metric="tput")
        df_metric = pd.concat([cpu_usage, tput], axis=1)
        df_metric["job_id"] = job_id
        df_metric["job_name"] = job_name
        df_metric.index = pd.to_timedelta(df_metric.index - df_metric.index.min())
        return df_metric
    
    def compile_alloy(self, query, output_dir, parallelism=1, disable_operator_chaining=True):
        with en.play_on(roles=self.roles, pattern_hosts=self.pattern_manager) as t:
            t.shell(f"docker run --rm -it -e EXECUTION_MODE=COMPILE -e NUM_SOURCES={parallelism} -e DISABLE_OPERATOR_CHAINING={disable_operator_chaining} -e JOBMANAGER_RPC_ADDRESS=test_jobmanager -e QUERY_FILENAME={query} --network alloy-test_default --mount type=bind,src=/root/config,dst=/opt/config alloy-control-plane:latest")
            t.fetch(flat="true",src="/root/config/alloy_plan.yaml", dest=f"{output_dir}/config/alloy_plan.yaml")
    
    def launch_table_api_run(self, job_name=None, duration=120, alloy=False, query=None, parallelism=1, disable_operator_chaining=True):
        execution_mode = "RUN_ALLOY" if alloy else "RUN"
        with en.play_on(roles=self.roles, pattern_hosts=self.pattern_manager) as t:
            t.shell("mkdir -p config")
            t.shell(
                f"docker run --rm -e EXECUTION_MODE={execution_mode} -e QUERY_FILENAME={query} -e NUM_SOURCES={parallelism} -e JOBMANAGER_RPC_ADDRESS=test_jobmanager -e DISABLE_OPERATOR_CHAINING={disable_operator_chaining} -e PARALLELISM={parallelism} --network alloy-test_default --mount type=bind,src=/root/config,dst=/opt/config alloy-control-plane:latest 2>&1; true",
            )
            #t.fetch(flat="true",src="/root/config/alloy_plan.yaml", dest="/home/dschmitz/alloy/xp/macro/full_stack/alloy_plan.yaml")
        
        start = datetime.datetime.now(tz=datetime.timezone.utc)
        print(f"Sleeping for {duration}s")
        time.sleep(duration)
        end =  datetime.datetime.now(tz=datetime.timezone.utc)
        cpu_usage = self.get_metrics_tunnel(start, end, metric_query='sum(container_cpu_usage_seconds_total{image!="", name=~"test_task.*|test_envoy.*|test_kafka-edge.*"}) by (name)')
        cpu_usage_rate = self.get_metrics_tunnel(start, end, metric_query='sum(irate(container_cpu_usage_seconds_total{image!="", name=~"test_task.*|test_envoy.*|test_kafka-edge.*"}[30s])) by (name)', metric="cpu_rate")
        tput = self.get_metrics_tunnel(start, end, metric_query='sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate)', metric="tput")
        network = self.get_metrics_tunnel(start, end, metric_query='sum(container_network_receive_bytes_total{image!="", name=~"test_task.*"}) by (name)', metric="network")
        network_rate = self.get_metrics_tunnel(start, end, metric_query='sum(irate(container_network_receive_bytes_total{image!="", name=~"test_task.*"}[30s])) by (name)', metric="network_rate")
        df_metric = pd.concat([cpu_usage, cpu_usage_rate, tput, network, network_rate], axis=1)
        df_metric["job_name"] = job_name
        df_metric.index = pd.to_timedelta(df_metric.index - df_metric.index.min())
        return df_metric

    def remove_stack(self, stack="test"):
        with en.play_on(roles=self.roles, pattern_hosts=self.pattern_manager) as t:
            if not isinstance(stack, list):
                stack = [stack]
            for stack_name in stack:
                t.shell(f"docker stack rm {stack_name}")
                