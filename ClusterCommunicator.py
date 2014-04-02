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
from TaskMapper import TaskMapper

class TaskAllocationMethod:
    ROUND_ROBIN = 0
    BROADCAST = 1

class TaskFarmer():
    
    def __init__(self, comm, **args ):
        self.task_queue = Queue()
        self.multithread = args.get( 'multithread', True )
        self.task_controller = TaskController( self.task_queue, comm )
        if self.multithread: self.task_controller.start()
        self.metadata = {}
        self.taskMapper = TaskMapper( self.task_controller.size )
        

    def post(self, task ):
        self.task_queue.put_nowait( task )
        
    def processTimeMetadata( self, task_metadata ):
        time_mdata = task_metadata.get('time', None )
        start_time =   getCompTime( time_mdata.get('start_time',None) )
        end_time =     getCompTime( time_mdata.get('end_time',None) )
        op_period_value =       time_mdata.get( 'period_value', None )   
        op_period_units = time_mdata.get( 'period_units', None )
        period = None if (op_period_value == None ) else ( op_period_value, op_period_units )
        op_time_length_value =       time_mdata.get( 'time_length', None )   
        op_time_length_units = time_mdata.get( 'time_length_units', None )
        time_length = None if (op_time_length_value == None ) else ( op_time_length_value, op_time_length_units )
        return start_time, end_time, period, time_length
                        
    def setMetadata( self, metadata ):
        self.metadata.extend( metadata )
            
    def createTasks( self, task_metadata ):
        operation_metadata = task_metadata['operation']  
        op_domain = operation_metadata['domain']  
        task_specs = []     
        if op_domain == OpDomain.TIME:
            start_time, end_time, op_period, op_time_length = self.processTimeMetadata( task_metadata )                    
            time_decomp = self.taskMapper.getTimeDecomposition( start_time, end_time, op_period, op_time_length )
            for ( time_slab, slab_index ) in time_decomp:
                time_metadata = task_metadata[ 'time' ]
                time_metadata['slabs'] = time_slab 
                time_metadata['index'] = slab_index 
                task_specs.append( task_metadata )               
        return task_specs;
                           
    def execute( self, task_metadata ): 
        task_specs = self.createTasks( task_metadata )
        for  task_spec in task_specs:
            if self.multithread:    self.task_queue.put_nowait( task_spec )
            else:                   self.task_controller.processTaskSpec( task_spec )
                  
class TaskController( threading.Thread ):

    def __init__(self, queue, comm=None, **args ): 
        super( TaskController, self ).__init__()      
        self.comm = comm
        self.rank = self.comm.Get_rank() if self.comm else 0
        self.size = self.comm.Get_size() if self.comm else 1
        self.local_task_exec = None if self.size > 1 else TaskExecutable()
        self.work_queue = queue
        self.active = True
        self.task_allocation_method = TaskAllocationMethod.ROUND_ROBIN
        self.setDaemon(True)
        assert self.rank == 0, "Controller rank ( %d ) must be 0" % self.rank
        
    def processTaskSpec(self,task):
        self.local_task_exec.processTaskSpec( task )  
                       
    def run(self):
        iproc = 0
        while self.active:
            iproc = iproc + 1
            if iproc == self.size: iproc = 1
            task = self.work_queue.get()
            
            if self.size == 1:                
                self.local_task_exec.processTaskSpec( task )               
            else:           
                if self.task_allocation_method == TaskAllocationMethod.ROUND_ROBIN:
                    self.comm.send( task, dest=iproc, tag=11)
                elif self.task_allocation_method == TaskAllocationMethod.BROADCAST:
                    pass 

#                 self.comm.bcast( task, root = 0 )
#     
#                 result = None
#                 result = self.comm.gather(result, root=0)
#     #            print "TaskExecutable-> message received: ", str( msg )
#                 reduced_result = task.reduce( result )
#                 self.work_queue.task_done()
#                 self.processResult( reduced_result, task )

    def stop(self):
        self.post( Task( { 'type' : 'Quit' } ) )  
        self.active = False  
        
    def processResult( self, reduced_result, task ):
        pass    
   
class TaskExecutable( threading.Thread ):

    def __init__(self, comm=None, **args ): 
        super( TaskExecutable, self ).__init__()      
        self.comm = comm
        self.rank = self.comm.Get_rank() if self.comm else 0 
        self.size = self.comm.Get_size() if self.comm else 1 
        self.task_allocation_method = TaskAllocationMethod.ROUND_ROBIN
        self.metadata = args
        self.active = True
        
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
        task = getTask( task_spec )
        if task: return task.map( self.rank, self.size )
        return -1
            
def getNodeApp(**args):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if ( not 'multithread' in args ):  args['multithread' ] = ( comm.size > 1 )
    hcomm = TaskFarmer(comm, **args) if ( rank == 0 ) else TaskExecutable( comm, **args )
    return hcomm
