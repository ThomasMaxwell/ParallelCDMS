'''
Created on Mar 27, 2014

@author: tpmaxwel
'''
import cdutil, cdms2, os, numpy, copy, time
from cdms2.selectors import Selector
from Utilities import *
    
class Task:
    
    def __init__(self, local_metadata ): 
        self.metadata = local_metadata
        
    def __getitem__(self, key):
        return self.metadata.get( key, None )
    
    def map( self, iproc, nprocs ):
        pass
        
    def reduce( self, data_array ):
        return None

class TemporalProcessing(Task):
    
    def __init__(self, task_spec, local_metadata={} ): 
        Task.__init__( self, local_metadata ) 
        self.task_metadata = task_spec 
        
    def getReducedAxes( self, slab_var, timestamp, reduced_axis_size=1 ):
        axes = []
        for iAxis in range( slab_var.rank() ):
            axis =  slab_var.getAxis( iAxis )
            if axis.isTime():
                if ( reduced_axis_size == len( axis ) ):
                    axes.append( axis )
                elif ( reduced_axis_size == 1 ):
                    dt = getCompTime(timestamp).sub(self.start_time)
                    rdt = dt.torel( units, axis.getCalendar() )
                    time_data = numpy.array( [rdt.], dtype = )
                    newTimeAxis = cdms2.createAxis( time_data, )
                    newTimeAxis.designateTime( 0, axis.getCalendar() )
                    newTimeAxis.id = "Time"
                    axes.append( newTimeAxis )
            else:
                axes.append( axis ) 
        return axes
                     

    def map( self, iproc, nprocs ):
        print "Proc %d: Running time processing." % iproc; sys.stdout.flush()
        tp0 = time.clock()
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
                
        tr0 = time.clock()
        results = self.runTimeProcessing( var )
        tr1 = time.clock()
        tr = tr1 - tr0

        tw0 = time.clock()
        global_time_index = self.slab_base_index
        output_path = os.path.join( output_dir, output_name )
        for ( timestamp, result_var ) in results:
            outfilename = "%s-%s.nc" % ( output_path, str(timestamp) )
            outfile = cdms2.createDataset( outfilename )
            outfile.write( result_var, None, None, None, None, None, None, global_time_index ) 
            global_time_index = global_time_index + 1
            outfile.close()
            print "Proc %d: Wrote %s slab to file %s" % ( iproc, str(result_var.shape), outfilename )
        tw1 = time.clock()
        tw = tw1 - tw0
        print "Proc %d:  Computed result, nslabs = %d, data prep time = %.3f sec, processing time = %.3f sec, write time = %.3f sec, total time = %.3f sec  " % ( iproc, len(results), tp, tr, tw, (tp+tr+tw) )
            
    def runTimeProcessing( self, var ):
        operation_metadata = self.task_metadata[ 'operation' ]
        opType = operation_metadata.get( 'type', TimeProcType.UNDEF ) 
        timeAxisIndex = var.getAxisIndex('time')       
        
        results = []
        has_bounds =  isinstance( self.time_list[0], tuple )
        num_steps = len( self.time_list ) if has_bounds else len( self.time_list ) - 1
        
        result = None
        for iTime in range( num_steps ):
            spec = self.time_list[iTime]
            ( t0, t1 ) = spec if has_bounds else ( spec, self.time_list[iTime+1] )  
            slice_var = var( time=(t0,t1,'co') )
               
            if ( opType == TimeProcType.SUM ):   
                result = numpy.sum( slice_var.data, axis = timeAxisIndex, keepdims=True  )
            elif ( opType == TimeProcType.MEAN ):
                result = numpy.mean( slice_var.data, axis = timeAxisIndex, keepdims=True  )
            elif ( opType == TimeProcType.MAX ):
                result = numpy.amax( slice_var.data, axis = timeAxisIndex, keepdims=True  )
            elif ( opType == TimeProcType.MIN ):
                result = numpy.amin( slice_var.data, axis = timeAxisIndex, keepdims=True  )
            elif opType == TimeProcType.SUBSET:   
                result = slice_var.data
                
            if not isNone(result): 
                rvar = cdms2.createVariable( result, id=var.id, copy=0, axes=self.getReducedAxes( slice_var, t0, result.shape[timeAxisIndex] ) )
                results.append( ( t0.replace(' ','_').replace(':','-'), rvar ) )
            
        return results
               
    def getSelector(self):  
        sel = None
        grid_metadata = self.task_metadata[ 'grid' ]
        time_metadata = self.task_metadata[ 'time' ]
        lat_bounds = grid_metadata.get( 'lat', None )
        self.time_list = time_metadata['slabs']
        self.start_time = getCompTime( self.time_list[0] )
        self.slab_base_index = time_metadata['index']
        if lat_bounds:
            if not isList( lat_bounds ): sel1 = Selector( latitude=lat_bounds )
            else: sel1 = Selector( latitude=lat_bounds[0] ) if ( len( lat_bounds ) == 1 ) else Selector( latitude=lat_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
        lon_bounds = grid_metadata.get( 'lon', None )
        if lon_bounds:
            if not isList( lon_bounds ): sel1 = Selector( longitude=lon_bounds )
            else: sel1 = Selector( longitude=lon_bounds[0] ) if ( len( lon_bounds ) == 1 ) else Selector( longitude=lon_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
        lev_bounds = grid_metadata.get( 'lev', None )
        if lev_bounds:
            if not isList( lev_bounds ): sel1 = Selector( level=lev_bounds )
            else: sel1 = Selector( level=lev_bounds[0] ) if ( len( lev_bounds ) == 1 ) else Selector( level=lev_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
        tsel = Selector( time=( self.time_list[0], self.time_list[-1] ) )
        sel= tsel if ( sel == None ) else sel & tsel    
        return sel
    
        
    def reduce( self, data_array ):
        return None


def getTask( task_spec ):
    operation_metadata = task_spec[ 'operation']
    task_domain = operation_metadata['domain'] 
    if task_domain == OpDomain.TIME: return TemporalProcessing( task_spec )   
    if task_domain == OpDomain.EXIT: return None




