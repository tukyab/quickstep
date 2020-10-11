import json
import numpy as np

data_size = 2
queries = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22"]
cores = [2,4]
query = 0
for q in range(len(queries)):
    with open("json_" + str(data_size) + "g/q" + queries[q] + "_c2.json", "r") as f:
        lines = f.readlines()

    adj_dict = {}
    i = 0
    num_operators = 0
    for line in lines:
        i += 1
        data = json.loads(line)
        if int(data["query id"]) == 0 and data["object"] == "operator":
            num_operators += 1
            edges = []
            for d in data["edges"]:
                edges.append(int(d["dst node id"]))
            adj_dict[int(data["operator id"])] = edges
        if data["object"] == "query":
            break

    adj_mat = []
    for op in range(num_operators):
        children = [0]*num_operators
        for i in adj_dict[op]:
            children[i] = 1
        adj_mat.append(children)

    task_dur_dict = {}
    for op in range(num_operators):
        task_dur_dict[op] = {'fresh_durations': {}, 'rest_wave': {}, 'first_wave': {}}

    for c in cores:
        with open("json_" + str(data_size) + "g/q" + queries[q] + "_c" + str(c) + ".json", "r") as f:
            lines = f.readlines()

        query_1 = [[] for _ in range(num_operators)]
        query_2 = [[] for _ in range(num_operators)]
        for line in lines:
            data = json.loads(line)
            if data["object"] == "work order":
                if int(data["query id"]) == 0:
                    query_1[int(data["operator id"])].append(int(data["time"]))
                elif int(data["query id"]) == 1:
                    query_2[int(data["operator id"])].append(int(data["time"]))
                else:
                    print("bad")

        for op in range(num_operators):
            task_dur_dict[op]['fresh_durations'][c] = query_1[op]
            task_dur_dict[op]['rest_wave'][c] = query_2[op]
            task_dur_dict[op]['first_wave'][c] = []

    np.save(str(data_size) + "g/adj_mat_" + str(q+1) + ".npy", np.array(adj_mat))
    np.save(str(data_size) + "g/task_duration_" + str(q+1) + ".npy", task_dur_dict)
