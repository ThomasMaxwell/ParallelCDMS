'''
Created on Jan 13, 2014

@author: tpmaxwel
'''

from mpi4py import MPI
from Queue import Queue
# from PyQt4 import QtCore
import time, threading, sys, os
from Utilities import control_message_signal
from Tasks import Task

class TaskAllocationMethod:
    ROUND_ROBIN = 0
    BROADCAST = 1

class TaskFarmer():
    
    def __init__(self, comm, **args ):
        self.task_queue = Queue()
        self.task_communicator = HControllerComm( comm, self.task_queue )
        self.task_communicator.start()
        self.metadata = {}

    def post(self, task ):
        self.task_queue.put_nowait( task )

    def processConfigData( self, config_data ): 
        bounds = config_data.get('bounds', None )
        if bounds:
            self.metadata['lat_bounds'] = bounds.get('lat',None)
            self.metadata['lon_bounds'] = bounds.get('lon',None)
            self.metadata['lev_bounds'] = bounds.get('lev',None)
            self.metadata['time_bounds'] = bounds.get('time',None)
        dataset = config_data.get('dataset', None )
        if dataset:
            self.metadata['dset_path'] = dataset.get( 'path', None )
            self.metadata['dset_id'] = dataset.get( 'id', self.metadata['dset_path'] )
            self.metadata['dset_vars'] = dataset.get( 'vars', "" ).split(',')
        operation = config_data.get('operation', None )
        if operation:
            self.metadata['op_class'] = dataset.get( 'class', None )
            self.metadata['op_type'] = dataset.get( 'type', None )
            self.metadata['op_period'] = dataset.get( 'period', None )
            
    def createTasks(self):
        pass
            
    def execute(self): 
        tasks = self.createTasks()
        for  task in tasks:
            self.task_communicator.post( task )
            
       
class HControllerComm( threading.Thread ):

    def __init__(self, comm, queue, **args ): 
        super( HControllerComm, self ).__init__()      
        self.comm = comm
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()
        self.work_queue = queue
        self.active = True
        self.task_allocation_method = TaskAllocationMethod.ROUND_ROBIN
        self.setDaemon(True)
        assert self.rank == 0, "Controller rank ( %d ) must be 0" % self.rank
                       
    def run(self):
        iproc = 0
        while self.active:
            iproc = iproc + 1
            if iproc == self.task_communicator.size: iproc = 1
            task = self.work_queue.get()
            
            if self.task_allocation_method == TaskAllocationMethod.ROUND_ROBIN:
                self.comm.send( task, dest=iproc, tag=11)
            elif self.task_allocation_method == TaskAllocationMethod.BROADCAST:
                pass 

#                 self.comm.bcast( task, root = 0 )
#     
#                 result = None
#                 result = self.comm.gather(result, root=0)
#     #            print "HCellComm-> message received: ", str( msg )
#                 reduced_result = task.reduce( result )
#                 self.work_queue.task_done()
#                 self.processResult( reduced_result, task )

    def stop(self):
        self.post( Task( { 'type' : 'Quit' } ) )  
        self.active = False  
        
    def processResult( self, reduced_result, task ):
        pass    
   
class HCellComm( threading.Thread ):

    def __init__(self, comm, **args ): 
        super( HCellComm, self ).__init__()      
        self.comm = comm
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()
        self.task_allocation_method = TaskAllocationMethod.ROUND_ROBIN
        self.metadata = args
        self.active = True
        assert self.rank <> 0, "Cell rank must not be 0" 
        
    def stop(self):
        self.active = False
        
    def run(self):
        while self.active:
            task = None
            if self.task_allocation_method == TaskAllocationMethod.BROADCAST:
                task = self.comm.bcast( task, root = 0 )
                if task.type == 'Quit': return 
                result = task.map( self.rank, self.size, self.metadata )
#                self.comm.gather(result, root=0)
            elif self.task_allocation_method == TaskAllocationMethod.ROUND_ROBIN:
                task = self.comm.recv( root = 0, tag=11 )
                if task.type == 'Quit': return 
                result = task.map( self.rank, self.size, self.metadata )
#                self.comm.gather(result, root=0)
#            print "HCellComm-> message received: ", str( msg )


#     
# class QHControllerComm( QtCore.QThread ):
# 
#     def __init__(self, comm, work_queue, **args ): 
#         super( HControllerComm, self ).__init__()      
#         self.comm = comm
#         self.rank = self.comm.Get_rank()
#         self.size = self.comm.Get_size()
#         self.active = True
#         self.work_queue = work_queue
#         self.setDaemon(True)
#         assert self.rank == 0, "Controller rank ( %d ) must be 0" % self.rank
#         
#     def stop(self):
#         self.active = False
#         
#     def run(self):
#         while self.active:
#             task = self.work_queue.get()
#             self.comm.bcast( task, root = 0 )
# 
#             result = None
#             result = self.comm.gather(result, root=0)
# #            print "HCellComm-> message received: ", str( msg )
#             reduced_result = task.reduce( result )
#             self.emit( control_message_signal, reduced_result )
#             self.work_queue.task_done()
# 
# class QHCellComm( QtCore.QThread ):
# 
#     def __init__(self, comm, **args ): 
#         super( HCellComm, self ).__init__()      
#         self.comm = comm
#         self.rank = self.comm.Get_rank()
#         self.size = self.comm.Get_size()
#         self.sleepTime = 0.01
#         self.active = True
#         assert self.rank <> 0, "Cell rank must not be 0" 
#         
#     def stop(self):
#         self.active = False
#         
#     def run(self):
#         while self.active:
#             msg = None
#             msg = self.comm.bcast( msg, root = 0 )
# #            print "HCellComm-> message received: ", str( msg )
#             self.emit( control_message_signal, msg )
#             if msg[ 'type' ] == 'Quit': 
#                 self.stop()               
#             else: 
#                 time.sleep( self.sleepTime )            

def getNodeApp():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()    
    hcomm = TaskFarmer(comm) if ( rank == 0 ) else HCellComm( comm )    
    return hcomm
