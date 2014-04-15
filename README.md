ParallelCDMS
============

Parallel map-reduce implementation of basic climate data processing operations using cdms and mpi.

Example execution:

> cd .../ParallelCDMS
> mpiexec -np 12 -hostfile ~/.mpi/hosts python Main.py -c ./config/ParallelCDMS.nasa-1.txt



