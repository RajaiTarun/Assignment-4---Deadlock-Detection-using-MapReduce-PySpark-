# main.py
import time
from pyspark import SparkContext, SparkConf
import os

# def parse_line(line):
#     parts = line.strip().split()
#     if len(parts) != 4:
#         return None
#     timestamp, level, process_id, resource_id = parts
#     return timestamp, level, process_id, resource_id

def parse_line(line):
    parts = line.strip().split()
    
    # Expect exactly 5 parts: date, time, action, process, resource
    if len(parts) != 5:
        return None
    
    date, time, action, process_id, resource_id = parts
    
    # Only care about WAIT and HOLD (ignore INFO, RELEASE, etc.)
    if action.upper() not in ["WAIT", "HOLD"]:
        return None
    
    # Return tuple in format your existing create_edge expects
    timestamp = f"{date} {time}"  # keep timestamp if needed for Phase 4 later
    return timestamp, action, process_id, resource_id


def create_edge(entry):
    """
    Converts a log entry into a directed edge for RAG.
    WAIT: Process → Resource
    HOLD: Resource → Process
    """
    timestamp, level, process_id, resource_id = entry
    if level.upper() == "WAIT":
        return (process_id, resource_id)
    elif level.upper() == "HOLD":
        return (resource_id, process_id)
    else:
        return None

def main():
    start_time = time.time()

    # 1️⃣ Spark setup
    conf = SparkConf().setAppName("DeadlockDetection").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # 2️⃣ RAG construction placeholder
    # TODO: Phase 1 & 2
    # Load the log file into an RDD
    log_file = "hpc_rag.log"  # make sure file is in project folder

    if not os.path.exists(log_file):
        print("Error: Log file not found!")
        sc.stop()  # stop SparkContext gracefully
        return
    
    lines_rdd = sc.textFile(log_file)
    # print(f"Total lines in log file: {lines_rdd.count()}")

    parsed_rdd = lines_rdd.map(parse_line).filter(lambda x: x is not None)
    # print(f"Total valid lines: {parsed_rdd.count()}")
    edges_rdd = parsed_rdd.map(create_edge).filter(lambda x: x is not None).distinct()
    # print(f"Total unique edges: {edges_rdd.count()}")

    # Extract all nodes
    nodes_rdd = edges_rdd.flatMap(lambda x: x)

    # Distinguish process and resource nodes
    process_nodes = nodes_rdd.filter(lambda n: n.startswith("P")).distinct()
    resource_nodes = nodes_rdd.filter(lambda n: n.startswith("R")).distinct()

    # Compute counts
    process_count = process_nodes.count()
    resource_count = resource_nodes.count()
    edge_count = edges_rdd.count()

    # Print exactly as required
    print(process_count)
    print(resource_count)
    print(edge_count)

    output = f"{process_count}\n{resource_count}\n{edge_count}\n"
    with open("metrics_output.txt", "w") as f:
        f.write(output)

    # 3️⃣ Cycle detection placeholder
    # 1️⃣ Build adjacency list: node -> list of neighbors
    # Example: (P105, R024) => P105 : [R024]
    adj_rdd = edges_rdd.groupByKey().mapValues(list)
    # adj_rdd.collect()  # for testing, but avoid on huge datasets

    # 2️⃣ Initialize paths as direct edges
    # Each path is a list of nodes [source, destination]
    paths_rdd = edges_rdd.map(lambda x: [x[0], x[1]])


    # 4️⃣ Metrics reporting placeholder
    # TODO: Phase 4 & 5

    sc.stop()
    end_time = time.time()
    print(round(end_time - start_time, 2))  # execution time

if __name__ == "__main__":
    main()
