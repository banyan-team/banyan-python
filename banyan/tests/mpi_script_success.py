from mpi4py import MPI


class Foo:
    def __init__(self, x):
        self.x = x
    
    def get_value(self):
        return (self.x + 1) ** 2

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

data = Foo(rank).get_value()
data = comm.gather(data, root=0)
if rank == 0:
    for i in range(size):
        assert data[i] == (i+1)**2
else:
    assert data is None

print("Finished executing script")