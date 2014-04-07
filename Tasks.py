'''
Created on Mar 27, 2014

@author: tpmaxwel
'''
import cdutil, cdms2, os, numpy, copy, time
from mpi4py import MPI
from cdms2.selectors import Selector
from Utilities import *
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
    
    task_comm = None
    
    def __init__(self, local_metadata ): 
        self.metadata = local_metadata
        if Task.task_comm == None:
            gcomm = MPI.COMM_WORLD
            rank = gcomm.Get_rank() 
            color = MPI.UNDEFINED if rank==0 else 1           
            Task.task_comm = gcomm.Split( color, rank-1 )
        
    def __getitem__(self, key):
        return self.metadata.get( key, None )
    
    def map( self, iproc, nprocs ):
        pass
        
    def reduce( self, data_array ):
        return None
    
    @classmethod
    def getSlabList( cls, task_spec ):
        time_metadata =  task_spec.get( 'time', None )
        if time_metadata == None: return " !! %s !!" % str( task_spec )
        return time_metadata.get( 'slabs', "[]")

    @classmethod
    def getTaskFromSpec( cls, task_spec ):
        operation_metadata = task_spec[ 'operation']
        task_domain = OpDomain.parseDomainSpec( operation_metadata['domain'] )
        if task_domain == OpDomain.TIME:
            return TemporalProcessing.getTask( task_spec )        
        if task_domain == OpDomain.EXIT: return None


class TemporalProcessing(Task):
    
    taskmap = {}

#     @classmethod
#     def getTask( cls, task_spec ):
#         operation_metadata = task_spec[ 'operation']
#         opType = TimeProcType.parseTypeSpec( operation_metadata.get( 'type', TimeProcType.UNDEF ) )
#         if ( opType == TimeProcType.SUM ):   
#             return TemporalSum( task_spec )  
#         elif ( opType == TimeProcType.MEAN ):
#             return TemporalAve( task_spec )  
#         elif ( opType == TimeProcType.MAX ):
#             return TemporalMax( task_spec )  
#         elif ( opType == TimeProcType.MIN ):
#             return TemporalMin( task_spec )  
#         elif opType == TimeProcType.SUBSET:   
#            return cls( task_spec )     


    @classmethod
    def getTask( cls, task_spec ):
        operation_metadata = task_spec[ 'operation']
        opType = operation_metadata.get( 'type', None ) 
        task = cls.taskmap.get( opType.lower(), None )
        return task( task_spec )

    @classmethod
    def register( cls, task_name, task_class ):
        if task_name in cls.taskmap:
            print>>sys.stderr, " Error: multiple tasks registered with name %s, some registration(s) ignored! " % task_name
        else:
            cls.taskmap[ task_name ] = task_class 
    
    def __init__(self, task_spec, local_metadata={} ): 
        Task.__init__( self, local_metadata ) 
        self.task_metadata = task_spec 
        
    def getReducedAxes( self, slab_var, timestamps, reduced_axis_size=1 ):
        axes = []
        for iAxis in range( slab_var.rank() ):
            axis =  slab_var.getAxis( iAxis )
            if axis.isTime():
                if ( reduced_axis_size == len( axis ) ):
                    axes.append( axis )
                elif ( reduced_axis_size == 1 ):
                    time_metadata = self.task_metadata[ 'time' ]
                    period_units = TimeUtil.parseTimeUnitSpec( time_metadata[ 'period_units' ] )
                    period_value = TimeUtil.parseRelTimeValueSpec( time_metadata[ 'period_value' ] )
                    time_length_value = TimeUtil.parseRelTimeValueSpec( time_metadata.get( 'time_length', period_value ) )  
                    time_length_units = TimeUtil.parseTimeUnitSpec( time_metadata.get( 'time_length_units', period_units ) )
                    time_data = []
                    bounds = []
                    for timestamp in timestamps:
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
                     

    def map( self, iproc, nprocs ):
        self.iproc = iproc
        self.nprocs = nprocs
        merge_timesteps = True
        print "Proc %d: Mapping Task." % iproc; sys.stdout.flush()
        tp0 = time.clock()
        self.processTimeMetadata()
        dataset_metadata = self.task_metadata[ 'dataset' ]
        dataset_path = dataset_metadata.get( 'path', None )
        operation_metadata = self.task_metadata[ 'operation' ]
        output_name = operation_metadata.get( 'name', "TemporalProcessing"  )
        output_dir = operation_metadata.get( 'dir', os.path.dirname( dataset_path )  )
        var_name = dataset_metadata.get( 'variable', None )
        ds = cdms2.open( dataset_path )
        var = ds[ var_name ]
        sel = self.getSelector()
        if sel: var = var(sel)
        tp1 = time.clock()
        tp = tp1 - tp0
        print "Proc %d: Done Mapping Task, time = %.3f" % ( iproc, tp ); sys.stdout.flush()               
        
        tr0 = time.clock()
        results = self.runTimeProcessing( var, merge_timesteps )
        tr1 = time.clock()
        tr = tr1 - tr0
        
        if merge_timesteps and ( nprocs > 1 ):
            merged_results = self.merge( results )
            results = [ merged_results ]

        tw0 = time.clock()
        global_time_index = self.slab_base_index
        output_path = os.path.join( output_dir, output_name )
        for ( timestamp, result_var ) in results:
            outfilename = "%s-%s.nc" % ( output_path, str(timestamp) )
            outfile = cdms2.createDataset( outfilename )
            outfile.write( result_var ) 
            global_time_index = global_time_index + 1
            outfile.close()
            time_axis = result_var.getTime()
            print "Proc %d: Wrote %s slab to file %s, time values = %s %s" % ( iproc, str(result_var.shape), outfilename, str( time_axis.getValue() ), time_axis.units )
        tw1 = time.clock()
        tw = tw1 - tw0
        print "Proc %d:  Computed result, nslabs = %d, data prep time = %.3f sec, processing time = %.3f sec, write time = %.3f sec, total time = %.3f sec  " % ( iproc, len(results), tp, tr, tw, (tp+tr+tw) )
            
#     def runTimeProcessing1( self, var ):
# #        print "Proc %d: Running time processing." % self.iproc; sys.stdout.flush()
#         operation_metadata = self.task_metadata[ 'operation' ]
#         self.timeAxisIndex = var.getAxisIndex('time')       
#         
#         results = []
#         has_bounds =  isinstance( self.time_list[0], tuple )
#         num_steps = len( self.time_list ) if has_bounds else len( self.time_list ) - 1
#         
#         result = None
#         time_steps = []
#         for iTime in range( num_steps ):
#             spec = self.time_list[iTime]
#             ( t0, t1 ) = spec if has_bounds else ( spec, self.time_list[iTime+1] )  
#             slice_var = var( time=(t0,t1,'co')  )
#             slice_array = numpy.ma.array( slice_var.data, mask=slice_var.mask )
#                
#             result = recoverLostDim( self.processDataSlice( slice_array ), self.timeAxisIndex, var.rank() )
#                 
#             if not isNone(result): 
#                 rvar = cdms2.createVariable( result, id=var.id, copy=0, axes=self.getReducedAxes( slice_var, t0, result.shape[ self.timeAxisIndex ] ) )
#                 results.append( ( t0.replace(' ','_').replace(':','-'), rvar ) )
#             
#         return results

    def runTimeProcessing( self, var, mergeResults = False ):
        print "Proc %d: Running time processing." % self.iproc; sys.stdout.flush()
        operation_metadata = self.task_metadata[ 'operation' ]
        self.timeAxisIndex = var.getAxisIndex('time')       
        
        results = []
        has_bounds =  isinstance( self.time_list[0], tuple )
        num_steps = len( self.time_list ) if has_bounds else len( self.time_list ) - 1
        
        result = None
        time_steps = []
        for iTime in range( num_steps ):
            spec = self.time_list[iTime]
            ( t0, t1 ) = spec if has_bounds else ( spec, self.time_list[iTime+1] )  
            slice_var = var( time=(t0,t1,'co')  )
            slice_array = numpy.ma.array( slice_var.data, mask=slice_var.mask )
             
            print "Proc %d: processDataSlice[%d]." % ( self.iproc, iTime ); sys.stdout.flush()
            processedData = self.processDataSlice( slice_array )  
            result = recoverLostDim( processedData, self.timeAxisIndex, var.rank() )
            
            if not isNone(result): 
                if mergeResults:
                    results.append( result )
                    time_steps.append( t0 )
                else:
                    rvar = cdms2.createVariable( result, id=var.id, copy=0, axes=self.getReducedAxes( slice_var, [ t0 ], result.shape[ self.timeAxisIndex ] ) )
                    results.append( ( t0.replace(' ','_').replace(':','-'), rvar ) )
       
        if mergeResults:    
            merged_result = numpy.concatenate( results, self.timeAxisIndex ) 
            gathered_result = merged_result # self.gather( merged_result )         
            rvar = cdms2.createVariable( gathered_result, id=var.id, copy=0, axes=self.getReducedAxes( slice_var, time_steps, results[0].shape[ self.timeAxisIndex ] ) ) 
            results = [ ( time_steps[0].replace(' ','_').replace(':','-'), rvar ) ]
                           
        return results

    
    def processTimeMetadata(self):
        time_metadata = self.task_metadata[ 'time' ]
        self.time_list = time_metadata['slabs']
        self.start_time = TimeUtil.getCompTime( self.time_list[0] )
        self.slab_base_index = time_metadata['index']
        self.nslab_map = numpy.array( time_metadata['nslab_map'], dtype='i') 
        self.global_start_time = TimeUtil.getCompTime( time_metadata['start_time'] )
               
    def getSelector(self):  
        sel = None
        grid_metadata = self.task_metadata[ 'grid' ]
        lat_bounds = OpDomain.parseBoundsSpec( grid_metadata.get( 'lat', None ) )
        if lat_bounds:
            if not isList( lat_bounds ): sel1 = Selector( latitude=lat_bounds )
            else: sel1 = Selector( latitude=lat_bounds[0] ) if ( len( lat_bounds ) == 1 ) else Selector( latitude=lat_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
        lon_bounds = OpDomain.parseBoundsSpec( grid_metadata.get( 'lon', None ) )
        if lon_bounds:
            if not isList( lon_bounds ): sel1 = Selector( longitude=lon_bounds )
            else: sel1 = Selector( longitude=lon_bounds[0] ) if ( len( lon_bounds ) == 1 ) else Selector( longitude=lon_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
        lev_bounds = OpDomain.parseBoundsSpec( grid_metadata.get( 'lev', None ) )
        if lev_bounds:
            if not isList( lev_bounds ): sel1 = Selector( level=lev_bounds )
            else: sel1 = Selector( level=lev_bounds[0] ) if ( len( lev_bounds ) == 1 ) else Selector( level=lev_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
        tsel = Selector( time=( self.time_list[0], self.time_list[-1] ) )
        sel= tsel if ( sel == None ) else sel & tsel    
        return sel
    
    def reduce( self, data_array ):
        return None
    
    def gather( self, data_array, proc_index = -1 ):
        new_shape = numpy.array( data_array.shape, dtype = 'f', copy=True )
        base_size, base_shape = getBaseSizeAndShape( data_array.shape, self.timeAxisIndex )
        nslabs = self.nslab_map.prod()
        new_shape[ self.timeAxisIndex ] = nslabs
        gathered_array = numpy.empty( [ new_shape.prod() ], dtype='f' )
        if proc_index < 0:
            Task.task_comm.AllGatherv( sendbuf=[ data_array.flat(), MPI.FLOAT ], recvbuf=[ gathered_array, (self.nslab_map*base_size, None), MPI.FLOAT] )
        else: 
            Task.task_comm.Gatherv( sendbuf=[ data_array.flat(), MPI.FLOAT ], recvbuf=[ gathered_array, (self.nslab_map*base_size, None), MPI.FLOAT], root=proc_index )
        result = None
        if (proc_index < 0) or ( proc_index == Task.task_comm.Get_rank() ):
            if self.timeAxisIndex == 0:
                result = gathered_array.reshape( new_shape )
            else:
                gathered_arrays = gathered_array.split( nslabs )
                for array in gathered_arrays: array.reshape(base_shape) 
                result = numpy.concatenate( gathered_arrays, self.timeAxisIndex )
        return result
    
    def processDataSlice( self, slice_array ):
        return slice_array

class TemporalSum(TemporalProcessing):
            
    def processDataSlice( self, slice_array ):
        return numpy.ma.sum( slice_array, axis = self.timeAxisIndex  )
    
TemporalProcessing.register( 'sum', TemporalSum )

class TemporalAve(TemporalProcessing):
            
    def processDataSlice( self, slice_array ):
        return numpy.ma.mean( slice_array, axis = self.timeAxisIndex  )

TemporalProcessing.register( 'ave', TemporalAve )

class TemporalMax(TemporalProcessing):
            
    def processDataSlice( self, slice_array ):
        return numpy.ma.max( slice_array, axis = self.timeAxisIndex  )

TemporalProcessing.register( 'max', TemporalMax )

class TemporalMin(TemporalProcessing):
            
    def processDataSlice( self, slice_array ):
        return numpy.ma.min( slice_array, axis = self.timeAxisIndex  )

TemporalProcessing.register( 'min', TemporalMin )

TemporalProcessing.register( 'sub', TemporalProcessing )
    



