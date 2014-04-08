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

    def map( self, global_comm, task_comm ):
        dataset_metadata = self.task_metadata[ 'dataset' ]
        dataset_path = dataset_metadata.get( 'path', None )
        operation_metadata = self.task_metadata[ 'operation' ]
        output_name = operation_metadata.get( 'name', "TemporalProcessing"  )
        output_dir = operation_metadata.get( 'dir', os.path.dirname( dataset_path )  )
        var_name = dataset_metadata.get( 'variable', None )
        time_specs = self.task_metadata[ 'time' ]
        grid_specs = self.task_metadata[ 'grid' ]        
        tr0 = time.clock()
        
        pvar = PVariable( task_comm, var_name, dataset_path, time_specs, grid_specs, global_comm = global_comm )
        pvar.execute( TemporalSum(), False )
        
        tr1 = time.clock()
        tr = tr1 - tr0
        tw0 = time.clock()       
        self.writeResults( output_dir, output_name, pvar )
        tw1 = time.clock()
        tw = tw1 - tw0
        print "Proc %d:  Computed result, nslabs = %d, processing time = %.3f sec, write time = %.3f sec, total time = %.3f sec  " % ( global_comm.Get_rank(), pvar.len(), tr, tw, (tr+tw) )
            
    def writeResults( self, output_dir, output_name, pvar ):
        global_time_index = pvar.getBaseTimeIndex()
        results = pvar.getLocalVariables()
        output_path = os.path.join( output_dir, output_name )
        for ( timestamp, result_var ) in results.items():
            outfilename = "%s-%s.nc" % ( output_path, str(timestamp) )
            outfile = cdms2.createDataset( outfilename )
            outfile.write( result_var ) 
            global_time_index = global_time_index + 1
            outfile.close()
            time_axis = result_var.getTime()
            print "Proc %d: Wrote %s slab to file %s, time values = %s %s" % ( pvar.global_comm.Get_rank(), str(result_var.shape), outfilename, str( time_axis.getValue() ), time_axis.units )
        
class computationalKernel():
    
    def __init__( self ): 
        pass
            
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
    



