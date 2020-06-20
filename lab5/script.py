from mpi4py import MPI
from mpi_master_slave import Master, Slave
from mpi_master_slave import WorkQueue
from enum import IntEnum
from math import inf

Tasks = IntEnum('Tasks', 'MAP REDUCE')


class MyApp(object):
    def __init__(self, slaves):
        self.master = Master(slaves)
        self.work_queue = WorkQueue(self.master)
        self.graph = {}
        self.temp_map_data = {}
        self.root = 3
        self.dist_changed = {}

    def read_graph(self):
        with open('Wiki-Vote.txt') as f:
            for line in f.readlines():
                from_node = int(line.strip().split('\t')[0])
                to_node = int(line.strip().split('\t')[1])

                if from_node not in self.graph:
                    self.graph[from_node] = {'d': inf, 'adj': [to_node]}
                else:
                    self.graph[from_node]['adj'].append(to_node)

                # in case the destination node will be never a source node
                if to_node not in self.graph:
                    self.graph[to_node] = {'d': inf, 'adj': []}

                # init distance changes
                if to_node not in self.dist_changed:
                    self.dist_changed[to_node] = True
                if from_node not in self.dist_changed:
                    self.dist_changed[from_node] = True

        self.graph[self.root]['d'] = 0

    def terminate_slaves(self):
        self.master.terminate_slaves()

    def run(self):
        self.read_graph()

        while True in self.dist_changed.values():

            # MAP
            for node in self.graph:
                self.work_queue.add_work(data=(Tasks.MAP, node, self.graph[node]))

            while not self.work_queue.done():
                self.work_queue.do_work()
                for nodes in self.work_queue.get_completed_work():
                    for n in nodes:
                        # array of distances
                        if n[0] not in self.temp_map_data:
                            self.temp_map_data[n[0]] = [n[1]]
                        else:
                            # don't add multiple times the same distance
                            if n[1] not in self.temp_map_data[n[0]]:
                                self.temp_map_data[n[0]].append(n[1])

            # REDUCE
            for node in self.temp_map_data:
                self.work_queue.add_work(data=(Tasks.REDUCE, node, self.temp_map_data[node]))
            while not self.work_queue.done():
                self.work_queue.do_work()
                for data in self.work_queue.get_completed_work():
                    node, d_min = data

                    # track distance changes
                    if d_min == self.graph[node]['d']:
                        self.dist_changed[node] = False
                    else:
                        self.graph[node]['d'] = d_min

            for n in self.graph:
                print(self.graph[n])


class MySlave(Slave):
    def __init__(self):
        super(MySlave, self).__init__()

    def do_work(self, data):
        task, node, struct = data

        if task == Tasks.MAP:
            to_emit = []
            for node in struct['adj']:
                to_emit.append((node, struct['d'] + 1))
            return to_emit
        elif task == Tasks.REDUCE:
            d_min = min(struct)
            return node, d_min


def main():
    rank = MPI.COMM_WORLD.Get_rank()
    size = MPI.COMM_WORLD.Get_size()

    if rank == 0:
        app = MyApp(slaves=range(1, size))
        app.run()
        app.terminate_slaves()
    else:

        MySlave().run()


if __name__ == "__main__":
    main()
