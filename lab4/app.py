from mpi4py import MPI
from mpi_master_slave import Master, Slave
from mpi_master_slave import WorkQueue
from enum import IntEnum
import itertools

Tasks = IntEnum('Tasks', 'MAP COMBINER REDUCE')


class MyApp(object):
    def __init__(self, slaves):
        self.master = Master(slaves)
        self.work_queue = WorkQueue(self.master)

    def terminate_slaves(self):
        self.master.terminate_slaves()

    def read_data(self, filename):
        transactions = []
        with open(filename) as f:
            lines = f.readlines()
            for i in range(len(lines)):
                tran_list = [int(tr) for tr in lines[i].strip().split(' ')]
                transactions.append({'tid': i, 'items': set(tran_list)})

        return transactions

    def run(self):
        # MAP
        transactions = self.read_data('retail.dat.txt')
        for t in transactions:
            self.work_queue.add_work(data=(Tasks.MAP, t))

        subsets = []
        while not self.work_queue.done():
            self.work_queue.do_work()
            for subst in self.work_queue.get_completed_work():
                subsets.extend(subst)

        # Combine
        slaves = self.master.get_ready_slaves()
        n = len(slaves)
        for i in range(0, len(subst), n):
            subst = subsets[i: i + n]
            self.master.run(slaves[i], data=(Tasks.COMBINER, subst))

        temp_data = {}
        for slave in self.master.get_completed_slaves():
            temp_data.update(self.master.get_data(slave))

        # Reduce
        temp_data = dict(sorted(temp_data.items()))
        for i in range(0, len(temp_data.items()), n):
            tmp = dict(temp_data.items()[i: i + n])
            self.master.run(slaves[i], data=(Tasks.REDUCE, tmp))

        ouput = {}
        for slave in self.master.get_completed_slaves():
            ouput.update(self.master.get_data(slave))


class MySlave(Slave):
    def __init__(self):
        super(MySlave, self).__init__()
        self.min_supp = 4

    def do_work(self, data):
        task, trans = data
        if task == Tasks.MAP:
            comb = []
            for m in range(1, len(trans['items'])):
                comb.extend(list(itertools.combinations(trans['items'], m)))
            return [{str(c): 1} for c in comb]
        elif task == Tasks.COMBINER:
            subst = trans
            temp = {}
            for sbst in subst:
                if sbst in temp:
                    temp[sbst] += 1
                else:
                    temp[sbst] = 1
            return temp
        elif task == Tasks.REDUCE:
            subst = trans
            temp = {}
            for sbst in subst:
                if sbst in temp:
                    temp[sbst] += subst[sbst]
                else:
                    temp[sbst] = subst[sbst]

            for key in temp:
                if temp[key] < self.min_supp:
                    del temp[key]
            return temp


def main():
    name = MPI.Get_processor_name()
    rank = MPI.COMM_WORLD.Get_rank()
    size = MPI.COMM_WORLD.Get_size()

    print('I am  %s rank %d (total %d)' % (name, rank, size))

    if rank == 0:
        app = MyApp(slaves=range(1, size))
        app.run()
        app.terminate_slaves()
    else:
        MySlave().run()

    print('Task completed (rank %d)' % rank)


if __name__ == "__main__":
    main()
