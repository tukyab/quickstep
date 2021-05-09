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

queries = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13"]
cores = [2,5,10,30]
operator = []
for data_size in [2, 5, 10, 20, 50, 80, 100]:
    for q in range(len(queries)):
        with open("/ssd1/tenzin/ssb/json_" + str(data_size) + "g/q" + queries[q] + "_c2.json", "r") as f:
            lines = f.readlines()

        adj_dict = {}
        edge_dict = {}
        num_operators = 0
        for line in lines:
            l = fix_esc(line);
            data = json.loads(l)
            if int(data["query id"]) == 0 and data["object"] == "operator":
                operator.append(data["name"])
                num_operators += 1
                edges = []
                for d in data["edges"]:
                    edges.append(int(d["dst node id"]))
                adj_dict[int(data["operator id"])] = edges

                edges_p = []
                for d in data["edges"]:
                    edges_p.append((int(d["dst node id"]), d["is pipeline breaker"] == "true"))
                edge_dict[int(data["operator id"])] = edges_p
            if data["object"] == "query":
                break

        adj_mat = []
        edge_mat = []
        for op in range(num_operators):
            children = [0]*num_operators
            for i in adj_dict[op]:
                children[i] = 1
            adj_mat.append(children)

            children2 = [True]*num_operators
            for i in edge_dict[op]:
                children2[i[0]] = i[1]
            edge_mat.append(children)

        task_dur_dict = {}
        task_mem_dict = {}
        for op in range(num_operators):
            task_dur_dict[op] = {'fresh_durations': {}, 'rest_wave': {}, 'first_wave': {}}
            task_mem_dict[op] = {'fresh_durations': {}, 'rest_wave': {}, 'first_wave': {}}

        for c in cores:
            with open("/ssd1/tenzin/ssb/json_" + str(data_size) + "g/q" + queries[q] + "_c" + str(c) + ".json", "r") as f:
                lines = f.readlines()

            query_1 = [[] for _ in range(num_operators)]
            query_2 = [[] for _ in range(num_operators)]
            query_1_mem = [[] for _ in range(num_operators)]
            query_2_mem = [[] for _ in range(num_operators)]
            for line in lines:
                l = fix_esc(line);
                #if len(l) > 4641:
                #    print(l[4641])
                #    print(l)
                data = json.loads(l)
                if data["object"] == "work order":
                    if int(data["query id"]) == 0:
                        query_1[int(data["operator id"])].append(int(int(data["time"])/1000))
                        query_1_mem[int(data["operator id"])].append(int(data["memory bytes"]))
                    elif int(data["query id"]) == 1:
                        query_2[int(data["operator id"])].append(int(int(data["time"])/1000))
                        query_2_mem[int(data["operator id"])].append(int(data["memory bytes"]))
                    else:
                        print("bad")

            for op in range(num_operators):
                task_dur_dict[op]['fresh_durations'][c] = query_1[op]
                task_dur_dict[op]['rest_wave'][c] = query_2[op]
                task_dur_dict[op]['first_wave'][c] = []
                task_mem_dict[op]['fresh_durations'][c] = query_1_mem[op]
                task_mem_dict[op]['rest_wave'][c] = query_2_mem[op]
                task_mem_dict[op]['first_wave'][c] = []

        np.save("/ssd1/tenzin/ssb/" + str(data_size) + "g/adj_mat_" + str(q+1) + ".npy", np.array(adj_mat))
        np.save("/ssd1/tenzin/ssb/" + str(data_size) + "g/task_duration_" + str(q+1) + ".npy", task_dur_dict)
        np.save("/ssd1/tenzin/ssb/" + str(data_size) + "g/task_memory_" + str(q+1) + ".npy", task_mem_dict)
        np.save("/ssd1/tenzin/ssb/" + str(data_size) + "g/edges_" + str(q+1) + ".npy", np.array(edge_mat))
        np.save("/ssd1/tenzin/ssb/" + str(data_size) + "g/operator_" + str(q+1) + ".npy", np.array(operator))
