import json
import numpy as np

data_size = 2
queries = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22"]
cores = [2,5,10,30]
query = 0
for q in range(len(queries)):
    with open("/flash2/tenzin/json_" + str(data_size) + "g/q" + queries[q] + "_c2.json", "r") as f:
        lines = f.readlines()

    adj_dict = {}
    num_operators = 0
    for line in lines:
        l = fix_esc(line);
        data = json.loads(l)
        if int(data["query id"]) == 0 and data["object"] == "operator":
            num_operators += 1
            edges = []
            for d in data["edges"]:
                edges.append((int(d["dst node id"]), d["is pipeline breaker"] == "true"))
            adj_dict[int(data["operator id"])] = edges
        if data["object"] == "query":
            break

    adj_mat = []
    for op in range(num_operators):
        children = [True]*num_operators
        for i in adj_dict[op]:
            children[i[0]] = i[1]
        adj_mat.append(children)

    np.save("/flash2/tenzin/" + str(data_size) + "g/edges_" + str(q+1) + ".npy", np.array(adj_mat))
