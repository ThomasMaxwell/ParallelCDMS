'''
Created on Mar 27, 2014

@author: tpmaxwel
'''
import cdutil, cdms2, os, numpy, copy, time, itertools
from mpi4py import MPI
from cdms2.selectors import Selector
from Utilities import *
from PVariable import PVariable
cdms2.setCompressionWarnings(False)

def recoverLostDim( array, dim_index, original_rank ):  
    shape = list( array.shape )
    if original_rank > len( shape ):
        shape.insert( dim_index, 1 )
        return array.reshape( shape )
    else: return array
       
def getBaseSizeAndShape( shape, time_index ):
    prod = 1
    base_shape = [1]*len(shape)
    for index, sval in enumerate(shape):
        if index <> time_index: 
            prod = prod * sval
            base_shape[index] = sval
    return prod, base_shape
        
class Task:

    
    def __init__(self, task_spec, local_metadata = {} ): 
        self.metadata = local_metadata
        self.task_metadata = task_spec 
        self.vars = {}
        
    def __getitem__(self, key):
        return self.metadata.get( key, None )
    
    def map( self, global_comm, task_comm ):
        pass
        
    def reduce( self, data_array ):
        return None

    @classmethod
    def getTaskFromSpec( cls, task_spec ):
        operation_metadata = task_spec[ 'operation']
        task_domain = OpDomain.parseDomainSpec( operation_metadata['domain'] )
        if task_domain == OpDomain.TIME:
            return TemporalProcessing( task_spec )        
        if task_domain == OpDomain.EXIT: return None


class TemporalProcessing(Task):
    
    taskmap = {}

    @classmethod
    def register( cls, task_name, task_class ):
        if task_name in cls.taskmap:
            print>>sys.stderr, " Error: multiple tasks registered with name %s, some registration(s) ignored! " % task_name
        else:
            cls.taskmap[ task_name ] = task_class

    @classmethod
    def getKernel( cls, task_name ):
        return cls.taskmap.get( task_name, None )
    
    def __init__(self, task_spec, local_metadata={} ): 
        Task.__init__( self, task_spec, local_metadata ) 

    def getAxes( self, slab_var, slabs = None ):
        axes = []
        for iAxis in range( slab_var.rank() ):
            axis =  slab_var.getAxis( iAxis )
            if axis.isTime():
                time_metadata = self.task_metadata[ 'time' ]
                period_units = TimeUtil.parseTimeUnitSpec( time_metadata[ 'period_units' ] )
                period_value = TimeUtil.parseRelTimeValueSpec( time_metadata[ 'period_value' ] )
                time_length_value = TimeUtil.parseRelTimeValueSpec( time_metadata.get( 'time_length', period_value ) )  
                time_length_units = TimeUtil.parseTimeUnitSpec( time_metadata.get( 'time_length_units', period_units ) )
                time_data = []
                bounds = []
                if slabs == None:
                    slabs = [ slab[0:-1] for slab in self.slab_list ]
                for slab in slabs:
                    if not isList( slab ): slab = [ slab ]
                    for timestamp in slab:
                        rdt0 = TimeUtil.getRelTime( timestamp, period_units, self.global_start_time, axis.getCalendar() )
                        rdt1 = rdt0.add( time_length_value, time_length_units )
                        time_data.append( rdt0.value )
                        bounds.append(  ( rdt0.value, rdt1.value )  )
                np_time_data = numpy.array(  time_data, dtype=numpy.float ) 
                np_bounds = numpy.array(  bounds, dtype=numpy.float ) 
                newTimeAxis = cdms2.createAxis( np_time_data, np_bounds )
                newTimeAxis.designateTime( 0, axis.getCalendar() )
                newTimeAxis.id = "Time"
                newTimeAxis.units = rdt0.units
                axes.append( newTimeAxis )
            else:
                axes.append( axis ) 
        return axes 
    
    def getPVar( self, var_name, global_comm, task_comm ):                   
        dataset_metadata = self.task_metadata[ 'dataset' ]
        dataset_path = dataset_metadata.get( 'path', None )
        time_specs = self.task_metadata[ 'time' ]
        grid_specs = self.task_metadata[ 'grid' ]        
        pvar = PVariable( task_comm, var_name, dataset_path, time_specs, grid_specs, global_comm = global_comm )
        return pvar

    def map( self, global_comm, task_comm ):      
        dataset_metadata = self.task_metadata[ 'dataset' ]
        var_name = dataset_metadata.get( 'variable', None )
        operation_metadata = self.task_metadata[ 'operation' ]
        task = operation_metadata['task']
        comp_kernel = self.getKernel( task )
        if comp_kernel <> None:       
            pvar = self.getPVar( var_name, global_comm, task_comm )        
            pvar.execute( comp_kernel( task_comm ), False )              
            self.writeResults( pvar )
        else:
            print>>sys.stderr, "Error, Unrecognized task: ", task
                
    def writeResults( self, pvar ):
        operation_metadata = self.task_metadata[ 'operation' ]
        output_name = operation_metadata.get( 'name', "TemporalProcessing"  )
        output_dir = operation_metadata.get( 'dir', os.path.dirname( pvar.dataset_path )  )
        results = pvar.getLocalVariables()
        output_path = os.path.join( output_dir, output_name )
        for ( timestamp, result_var ) in results.items():
            outfilename = "%s-%s.nc" % ( output_path, str(timestamp) )
            outfile = cdms2.createDataset( outfilename )
            outfile.write( result_var ) 
            outfile.close()
            time_axis = result_var.getTime()
            print "Proc %d: Wrote %s slab to file %s, time values = %s %s" % ( pvar.global_comm.Get_rank(), str(result_var.shape), outfilename, str( time_axis.getValue() ), time_axis.units )
        
class computationalKernel():
    
    def __init__( self, task_comm ): 
        self.task_comm = task_comm
        
    @property
    def worker_rank(self):
        return  self.task_comm.Get_rank()   

    @property
    def nworkers(self):
        return  self.task_comm.Get_size()   
            
class TemporalSum(computationalKernel):
            
    def execute( self, slice_array, **args ):
        return numpy.ma.sum( slice_array, **args  )
    
TemporalProcessing.register( 'sum', TemporalSum )

class TemporalAve(computationalKernel):
            
    def execute( self, slice_array, **args ):
        return numpy.ma.mean( slice_array, **args  )

TemporalProcessing.register( 'ave', TemporalAve )

class TemporalMax(computationalKernel):
            
    def execute( self, slice_array, **args ):
        return numpy.ma.max( slice_array, **args  )

TemporalProcessing.register( 'max', TemporalMax )

class TemporalMin(computationalKernel):
            
    def execute( self, slice_array, **args ):
        return numpy.ma.min( slice_array, **args  )

TemporalProcessing.register( 'min', TemporalMin )

TemporalProcessing.register( 'sub', TemporalProcessing )
    



