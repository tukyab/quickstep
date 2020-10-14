import json
import numpy as np

def fix_esc(line):
    l = line.replace("\\2", "\\\\ 2")
    l = l.replace("\\3", "\\\\ 3")
    l = l.replace("\\0", "\\\\ 0")
    l = l.replace("\\1", "\\\\ 1")
    l = l.replace("\\4", "\\\\ 4")
    l = l.replace("\\5", "\\\\ 5")
    l = l.replace("\\6", "\\\\ 6")
    l = l.replace("\\7", "\\\\ 7")
    l = l.replace("\\8", "\\\\ 8")
    l = l.replace("\\9", "\\\\ 9")
    l = l.replace("\\@", "\\\\ @")
    l = l.replace("\\#", "\\\\ #")
    l = l.replace("\\a", "\\\\ a")
    l = l.replace("\\b", "\\\\ b")
    l = l.replace("\\c", "\\\\ c")
    l = l.replace("\\d", "\\\\ d")
    l = l.replace("\\e", "\\\\ e")
    l = l.replace("\\f", "\\\\ f")
    l = l.replace("\\g", "\\\\ g")
    l = l.replace("\\h", "\\\\ h")
    l = l.replace("\\i", "\\\\ i")
    l = l.replace("\\j", "\\\\ j")
    l = l.replace("\\k", "\\\\ k")
    l = l.replace("\\l", "\\\\ l")
    l = l.replace("\\m", "\\\\ m")
    l = l.replace("\\o", "\\\\ o")
    l = l.replace("\\p", "\\\\ p")
    l = l.replace("\\q", "\\\\ q")
    l = l.replace("\\r", "\\\\ r")
    l = l.replace("\\s", "\\\\ s")
    l = l.replace("\\t", "\\\\ t")
    l = l.replace("\\u", "\\\\ u")
    l = l.replace("\\v", "\\\\ v")
    l = l.replace("\\w", "\\\\ w")
    l = l.replace("\\x", "\\\\ x")
    l = l.replace("\\y", "\\\\ y")
    l = l.replace("\\z", "\\\\ z")
    l = l.replace("\\%", "\\\\ %")
    l = l.replace("\\'", "\\\\'")
    return l

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
        with open("/flash2/tenzin/json_" + str(data_size) + "g/q" + queries[q] + "_c" + str(c) + ".json", "r") as f:
            lines = f.readlines()

        query_1 = [[] for _ in range(num_operators)]
        query_2 = [[] for _ in range(num_operators)]
        for line in lines:
            l = fix_esc(line);
            #if len(l) > 4641:
            #    print(l[4641])
            #    print(l)
            data = json.loads(l)
            if data["object"] == "work order":
                if int(data["query id"]) == 0:
                    query_1[int(data["operator id"])].append(int(int(data["time"])/1000))
                elif int(data["query id"]) == 1:
                    query_2[int(data["operator id"])].append(int(int(data["time"])/1000))
                else:
                    print("bad")

        for op in range(num_operators):
            task_dur_dict[op]['fresh_durations'][c] = query_1[op]
            task_dur_dict[op]['rest_wave'][c] = query_2[op]
            task_dur_dict[op]['first_wave'][c] = []

    np.save("/flash2/tenzin/" + str(data_size) + "g/adj_mat_" + str(q+1) + ".npy", np.array(adj_mat))
    np.save("/flash2/tenzin/" + str(data_size) + "g/task_duration_" + str(q+1) + ".npy", task_dur_dict)
