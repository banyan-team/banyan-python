from mpi4py import MPI


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# Raise exception on all workers except for main
raise Exception(f"Error on worker {rank}")