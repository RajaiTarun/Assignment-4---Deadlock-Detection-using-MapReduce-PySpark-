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
    

def extend_paths(path, adj_dict):
    """
    Extend a path by one step using the adjacency list.
    Avoid revisiting nodes to prevent infinite loops.
    """
    last_node = path[-1]
    extensions = []

    for neighbor in adj_dict.get(last_node, []):
        if neighbor not in path:  # avoid revisits
            extensions.append(path + [neighbor])
        else:
            # Cycle detected, but we keep full cycles for later
            extensions.append(path + [neighbor])
    return extensions

# def normalize_cycle(cycle):
#     """
#     Rotate cycle so that the smallest node (lexicographically) is first.
#     This helps deduplicate cycles that are the same but start at different nodes.
#     """
#     cycle = list(cycle)
#     n = len(cycle)
#     # Find the index of the smallest node
#     min_index = min(range(n), key=lambda i: cycle[i])
#     # Rotate
#     normalized = cycle[min_index:] + cycle[:min_index]
#     return tuple(normalized)

def normalize_cycle(cycle):
    """
    Rotate cycle so that the smallest node (lexicographically) is first.
    This helps deduplicate cycles that are the same but start at different nodes.
    """
    # The 'cycle' tuple includes the repeated start node (e.g., A->B->A)
    # We must remove that repeated node before normalizing.
    nodes_in_cycle = list(cycle)[:-1]
    
    n = len(nodes_in_cycle)
    # Find the index of the smallest node
    min_index = min(range(n), key=lambda i: nodes_in_cycle[i])
    
    # Rotate the list *without* the repeated node
    normalized = nodes_in_cycle[min_index:] + nodes_in_cycle[:min_index]
    return tuple(normalized)

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
    # paths_rdd = edges_rdd.map(lambda x: [x[0], x[1]])

    # changing path_rdd logic
    paths_rdd = edges_rdd.filter(lambda x: x[0].startswith("P")).map(lambda x: [x[0], x[1]])


    # Max path length to prevent infinite loops
    # ⚠️ Note: This approach works if the graph isn’t huge (≤50,000 edges). For a true MapReduce scalable solution, we would propagate paths using RDD transformations instead of collecting everything locally. But for assignment purposes, this logic is enough and simpler to debug.

    MAX_PATH_LEN = 25

    # Convert adjacency RDD to dictionary for efficient lookup
    # adj_dict = dict(adj_rdd.collect())  # small enough for local usage
    # changing
    adj_rdd = edges_rdd.groupByKey().mapValues(lambda lst: list(set(lst)))
    adj_dict = dict(adj_rdd.collect())

    # Initialize paths with direct edges
    # current_paths = paths_rdd.collect()  # list of lists

    # all_cycles = set()  # to store detected cycles as tuples

    # for step in range(MAX_PATH_LEN):
    #     new_paths = []
    #     for path in current_paths:
    #         last_node = path[-1]
    #         if last_node in adj_dict:
    #             for neighbor in adj_dict[last_node]:
    #                 if neighbor == path[0]:
    #                     # Cycle detected
    #                     all_cycles.add(tuple(path + [neighbor]))
    #                 elif neighbor not in path:
    #                     new_paths.append(path + [neighbor])
    #     # Stop if no new paths to extend
    #     if not new_paths:
    #         break
    #     current_paths = new_paths
    all_cycles = set()
    current_paths = paths_rdd.collect()  # list of lists

    for step in range(MAX_PATH_LEN):
        new_paths = []
        # for path in current_paths:
        #     last_node = path[-1]
        #     for neighbor in adj_dict.get(last_node, []):
        #         if neighbor == path[0]:
        #             # Cycle completed → add it
        #             all_cycles.add(tuple(path + [neighbor]))
        #             # Do NOT extend further
        #         elif neighbor not in path:
        #             # Only extend if neighbor is NOT in path at all
        #             new_paths.append(path + [neighbor])
        for path in current_paths:
            last_node = path[-1]
            for neighbor in adj_dict.get(last_node, []):
                if neighbor == path[0]:
                    # Completed cycle → add it
                    all_cycles.add(tuple(path + [neighbor]))
                    # Do NOT extend this path
                elif neighbor not in path:
                    # Safe to extend
                    new_paths.append(path + [neighbor])
                # If neighbor is in path but not start → skip it (prevents internal repeats)
        if not new_paths:
            break
        current_paths = new_paths
        # print(f"Step {step}, paths: {current_paths}")

    print(f"Total raw cycles found: {len(all_cycles)}")

    # Normalize all cycles
    normalized_cycles = [normalize_cycle(c) for c in all_cycles]

    # Deduplicate
    unique_cycles = list(set(normalized_cycles))

    # Sort: first by length, then lexicographically
    unique_cycles.sort(key=lambda x: (len(x), x))

    # Print deadlock count
    print(len(unique_cycles))

    # Print each cycle in the required format (space-separated)
    for cycle in unique_cycles:
        print(" ".join(cycle))

    # Flatten all cycles and keep only process IDs
    process_set = set()
    for cycle in unique_cycles:
        for node in cycle:
            if node.startswith("P"):
                process_set.add(node)

    # Sort alphabetically
    sorted_processes = sorted(process_set)

    # Print total unique process count
    print(len(sorted_processes))

    # Print all process IDs in required format
    print(" ".join(sorted_processes))

    # 4️⃣ Metrics reporting placeholder
    # TODO: Phase 4 & 5

    sc.stop()
    end_time = time.time()
    print(round(end_time - start_time, 2))  # execution time

if __name__ == "__main__":
    main()
