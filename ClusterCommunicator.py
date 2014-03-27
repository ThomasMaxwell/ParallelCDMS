'''
Created on Jan 13, 2014

@author: tpmaxwel
'''

from mpi4py import MPI
from Queue import Queue
# from PyQt4 import QtCore
import time, threading
from Utilities import control_message_signal
from Tasks import Task
    
class HControllerComm( threading.Thread ):

    def __init__(self, comm, queue, **args ): 
        super( HControllerComm, self ).__init__()      
        self.comm = comm
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()
        self.work_queue = queue
        self.active = True
        self.setDaemon(True)
        assert self.rank == 0, "Controller rank ( %d ) must be 0" % self.rank
        
    def post(self, task ):
        self.work_queue.put_nowait( task )
                
    def run(self):
        while self.active:
            task = self.work_queue.get()
            self.comm.bcast( task, root = 0 )

            result = None
            result = self.comm.gather(result, root=0)
#            print "HCellComm-> message received: ", str( msg )
            reduced_result = task.reduce( result )
            self.work_queue.task_done()
            self.processResult( reduced_result, task )

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
        self.metadata = args
        self.active = True
        assert self.rank <> 0, "Cell rank must not be 0" 
        
    def stop(self):
        self.active = False
        
    def run(self):
        while self.active:
            task = None
            task = self.comm.bcast( task, root = 0 )
            if task.type == 'Quit': return 
            result = task.map( self.rank, self.size, self.metadata )
            self.comm.gather(result, root=0)
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

def getHComm():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()    
    hcomm = HControllerComm(comm) if ( rank == 0 ) else HCellComm( comm )    
    return hcomm
