'''
Created on Jan 13, 2014

@author: tpmaxwel
'''

from mpi4py import MPI
from Queue import Queue
import cdutil, cdtime
import time, threading, sys, os
from Utilities import *
from Tasks import Task, getTask
from TaskMapper import TaskMapper, TimeMgr

class TaskAllocationMethod:
    ROUND_ROBIN = 0
    BROADCAST = 1

class TaskFarmer():
    
    def __init__(self, comm, **args ):
        self.task_queue = Queue()
        self.task_communicator = HControllerComm( comm, self.task_queue )
        self.task_communicator.start()
        self.metadata = {}
        self.taskMapper = TaskMapper( self.task_communicator.size )

    def post(self, task ):
        self.task_queue.put_nowait( task )

#     def processConfigData( self, config_data ): 
#         bounds = config_data.get('bounds', None )
#         self.
#         if bounds: self.setBoundsMetadata( bounds )
#         
#         dataset = config_data.get('dataset', None )
#         if dataset:
#             self.metadata['dset_path'] = dataset.get( 'path', None )
#             self.metadata['dset_id'] = dataset.get( 'id', self.metadata['dset_path'] )
#             self.metadata['dset_vars'] = dataset.get( 'vars', "" ).split(',')
#         operation = config_data.get('operation', None )
#         if operation:
#             self.metadata['op_domain'] = dataset.get( 'domain', None )
#             self.metadata['op_type'] = dataset.get( 'type', None )
#             self.metadata['op_period'] = dataset.get( 'period', None )
#             self.metadata['op_period_units'] = dataset.get( 'units', None )
        
    def processTimeMetadata( self, task_metadata ):
        time_mdata = task_metadata.get('time', None )
        start_time =   getCompTime( time_mdata.get('start_time',None) )
        end_time =     getCompTime( time_mdata.get('end_time',None) )
        op_mdata = task_metadata.get('operation', None )
        op_period =       op_mdata.get( 'period', None )   
        op_period_units = op_mdata.get( 'period_units', None )
        return start_time, end_time, op_period, op_period_units
                        
    def setMetadata( self, metadata ):
        self.metadata.extend( metadata )
            
    def createTasks( self, task_metadata ):
        op_domain = task_metadata['op_domain']  
        task_specs = []     
        if op_domain.lower() == "time":
            start_time, end_time, op_period, op_period_units = self.processTimeMetadata( task_metadata )                    
            time_decomp = self.taskMapper.getTimeDecomposition( start_time, end_time, op_period, op_period_units )
            for time_slab in time_decomp:
                task_specs.append( ( time_slab, task_metadata ) )               
        return task_specs;
                           
    def execute( self, task_metadata ): 
        task_specs = self.createTasks( task_metadata )
        for  task_spec in task_specs:
            self.task_queue.post( task_spec )
                   
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
                task_spec = self.comm.bcast( task, root = 0 )
                rv = self.processTaskSpec( task_spec )
                if rv == -1: return

            elif self.task_allocation_method == TaskAllocationMethod.ROUND_ROBIN:
                task_spec = self.comm.recv( root = 0, tag=11 )
                rv = self.processTaskSpec( task_spec )
                if rv == -1: return



    def processTaskSpec( self, task_spec ):
        task = getTask( task_spec[1:] )
        if task: return task.map( self.rank, self.size )
        return -1
            
            
    
        
#                self.comm.gather(result, root=0)

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
    hcomm.createTasks()   
    return hcomm

if __name__ == "__main__":
    averager = cdutil.FEB
    print "x"
