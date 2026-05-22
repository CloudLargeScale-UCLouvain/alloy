import json
import os
import time
import yaml
from alloy import Graph,write_alloy_plan 
import json 

alloy_filename = "/opt/config/alloy_plan.json"
alloy_yaml = "/opt/config/alloy_plan.yaml"
filename = "/opt/config/test.json"
table_filename = f'/opt/config/{os.environ.get("TABLES_FILENAME","tables.sql")}'
query_filename = f'/opt/config/{os.environ.get("QUERY_FILENAME","query.sql")}'

def create_tables(t_env):
    with open(table_filename) as f:
        sql = f.read()
        for statement in sql.split(";"):
            if statement.strip():
                t_env.execute_sql(statement)

def setup_table_env():
    from pathlib import Path

    from pyflink.java_gateway import get_gateway
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    gateway = get_gateway()
    string_class = gateway.jvm.String
    string_array = gateway.new_array(string_class, 0)
    stream_env = gateway.jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
    j_stream_exection_environment = stream_env.createRemoteEnvironment(
        f"{os.environ['JOBMANAGER_RPC_ADDRESS']}", 
        8081, 
        string_array
    )
    env = StreamExecutionEnvironment(j_stream_exection_environment)
    env.set_parallelism(int(os.environ.get("PARALLELISM", "1")))
    if os.environ.get("DISABLE_OPERATOR_CHAINING", 'False').lower() in ('true', '1', 't'):
        env.disable_operator_chaining()
    table_env = StreamTableEnvironment.create(env)

    # Generate a proper file URI for the jar in the current directory
    jar_uri = Path("/opt/flink-sql-connector-kafka-1.16.2.jar").absolute().as_uri()
    print(jar_uri)

    t_env=table_env
    t_env.get_config().set(
        "pipeline.classpaths", jar_uri
        #"pipeline.jar", "file:///opt/flink-sql-connector-kafka-1.16.2.jar"
    )
    table_env.get_config().set("parallelism.default", os.environ.get("PARALLELISM", "1"))

    t_env.get_config().get_configuration().set_string("table.plan.force-recompile", "true")
    return t_env

def setup_sql_tables(t_env):
    # Create source and sink tables
    create_tables(t_env)

    # Drop the catalog if it already exists to avoid conflicts.
    t_env.execute_sql('DROP CATALOG IF EXISTS alloy;')
    # Use SQL to query the source table and insert the results into the sink table.
    # In this example, we select rows where id > 10.

    with open(query_filename) as f:
        query_sql = f.read()

    insert_sql = f"""
    COMPILE PLAN '{filename}' FOR
    {query_sql}
    """

    try:
        os.remove(filename)
    except FileNotFoundError:
        pass
    
    t_env.execute_sql(insert_sql)

def compile_plan():
    with open(filename, "r") as f:
        plan = json.load(f)

    graph = Graph(plan)

    alloy_plan = graph.get_alloy_plan()

    print(alloy_plan)
    new_plan = graph.apply_alloy_plan(alloy_plan, apply_partition=False)

    with open(alloy_filename, "w+") as f:
        f.write(new_plan)
    envoy_yaml = write_alloy_plan(alloy_plan, apply_partition=False)
    # set num_sources

    num_sources = os.environ.get("NUM_SOURCES", "2")
    for topic in envoy_yaml:
        envoy_yaml[topic]["num_sources"] = int(num_sources)
        envoy_yaml[topic]["num_partitions"] = int(num_sources)
    print(envoy_yaml)
    with open(alloy_yaml, 'w+') as file:
        yaml.dump(envoy_yaml, file)

def run_plan(t_env, alloy=False):
    if alloy:
        t_env.execute_sql(f"EXECUTE PLAN '{alloy_filename}'")
    else:
        t_env.execute_sql(f"EXECUTE PLAN '{filename}'")
    
# The first attempts typically fail while Flink is starting up, but it will eventually succeed.
MAX_RETRIES = 30

def main():
    for attempt in range(MAX_RETRIES):
        t_env = setup_table_env()
        try:
            setup_sql_tables(t_env)
            break
        except Exception as e:
            print(f"Error executing SQL (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt + 1 == MAX_RETRIES:
                raise
            time.sleep(2)
    execution_mode = os.environ.get("EXECUTION_MODE","RUN")
    if execution_mode == "COMPILE":
        compile_plan()
    elif execution_mode == "RUN_ALLOY":   
        run_plan(t_env, alloy=True)
    elif execution_mode == "RUN":   
        compile_plan()
        run_plan(t_env, alloy=False)
    else:
        raise ValueError(f"Unknown EXECUTION_MODE: {execution_mode}")

if __name__ == "__main__":
    main()