'''
Created on Apr 8, 2014

@author: tpmaxwel
'''
import cdutil, cdms2, os, numpy, copy, time, itertools
from mpi4py import MPI
from cdms2.selectors import Selector
from Utilities import *

def serializeSpec( spec ):
    if isinstance( spec, list ) or isinstance( spec, tuple ):
        return [ str(item) for item in spec ]
    else: return str( spec )
    
class DecompositionMode:
    NDEF = 0
    Temporal = 1
    Spatial = 2

class ParallelDecomposition:
    
    def __init__( self, work_rank, nworkers ):
        self.nworkers = nworkers
        self.work_rank = work_rank
        self.decomp_mode = DecompositionMode.NDEF
        
    def getDecompositionMode(self):
        return self.decomp_mode
        
    def generateDecomposition( self, decomp_mode, task_metadata ):
        spatial_metadata = task_metadata[ 'grid' ]
        self.processSpatialMetadata( spatial_metadata )
        time_metadata = task_metadata[ 'time' ]        
        self.processTimeMetadata( time_metadata )
        if decomp_mode == DecompositionMode.Spatial:
            self.generateSpatialDecomposition()
        elif decomp_mode == DecompositionMode.Temporal:
            self.generateTimeDecomposition()

    def processTimeMetadata( self, time_metadata ):
        self.global_start_time =    TimeUtil.getCompTime( time_metadata['start_time'] )
        self.global_end_time =      TimeUtil.getCompTime( time_metadata['end_time'] )
        self.period_units =         TimeUtil.parseTimeUnitSpec( time_metadata[ 'period_units' ] )
        self.period_value =         TimeUtil.parseRelTimeValueSpec( time_metadata[ 'period_value' ] )
        self.slice_length_value =   TimeUtil.parseRelTimeValueSpec( time_metadata.get( 'time_length', self.period_value ) )  
        self.slice_length_units =   TimeUtil.parseTimeUnitSpec( time_metadata.get( 'time_length_units', self.period_units ) )

    def getGlobalTimeBounds(self):
        return ( self.global_start_time, self.global_end_time )

    def processSpatialMetadata( self, space_metadata ):
        self.lat_bounds = [ float(lval) for lval in space_metadata.get('lat',[]) ]
        self.lon_bounds = [ float(lval) for lval in space_metadata.get('lon',[]) ]
     
    def getNumberOfSlices(self):   
        return len( self.worker_slice_allocation ) 

    def getSlice( self, slice_index ):   
        return self.worker_slice_allocation[slice_index]

    def generateSpatialDecomposition( self ):
        dLonGlobal = self.lon_bounds[1] - self.lon_bounds[0]
        dLonWorker = dLonGlobal/self.nworkers
        self.spatial_slices = []
        Lon0 = self.lon_bounds[0]
        for iWorker in range( self.nworkers ):
            Lon1 = Lon0 + dLonWorker
            self.spatial_slices.append( (Lon0,Lon1) )
            Lon0 = Lon1
        self.worker_spatial_allocation = self.spatial_slices[ self.work_rank ]
        self.decomp_mode = DecompositionMode.Spatial
        
    def getWorkerSpaceBounds(self):
        return self.worker_spatial_allocation
   
    def generateTimeDecomposition( self ):
        t0 = self.global_start_time 
        self.time_slices = []
        while True:
            if self.slice_length_value == None:
                self.time_slices.append( t0 )
                t1 = t0
            else:
                t1 = t0.add( self.slice_length_value, self.slice_length_units )
                if t1.cmp( self.global_end_time ) > 0: break   
                self.time_slices.append( ( t0, t1 ) )
            
            t0 = t0.add( self.slice_length_value, self.slice_length_units )
            if t0.cmp( self.global_end_time ) > 0: break                 

        self.nslices = len( self.time_slices )
        if self.nslices > self.nworkers:
            nexcess_slices = self.nslices / self.nworkers 
            nexcess_slices_rem = self.nslices - nexcess_slices*self.nworkers - 1
            nslice_map = [ nexcess_slices if ( iSlice >= nexcess_slices_rem ) else nexcess_slices + 1 for iSlice in range(self.nworkers) ]
        else:
            nslice_map = [ 1 ] * self.nslices
        
        self.slice_index_offsets = [ ] 
        base_slice_index = 0  
        self.slice_decomp_list = []
        for iProc in range( len( nslice_map ) ):
            try:
                nslices = nslice_map[ iProc ] 
                slice_spec_start = self.time_slices[ base_slice_index ]
                time_list = [ serializeSpec( slice_spec_start ) ]
                n_additional_slices = nslices if (self.slice_length_value == None) else nslices-1
                for iSlice in range( n_additional_slices ):
                    slice_spec = self.time_slices[ base_slice_index + iSlice + 1 ]
                    time_list.append( serializeSpec( slice_spec ) )
                self.slice_index_offsets.append( base_slice_index )
                self.slice_decomp_list.append( time_list )
                base_slice_index = base_slice_index + nslices
            except Exception, err:
                print str(err) 
        self.worker_slice_allocation = self.slice_decomp_list[ self.work_rank ]      
        self.slice_base_time_index =self.slice_index_offsets[ self.work_rank ]
        self.nslice_map = numpy.array( nslice_map, dtype='i') 
        self.start_time = TimeUtil.getCompTime( self.worker_slice_allocation[0][0] )
        self.decomp_mode = DecompositionMode.Temporal
        
    def getWorkerTimeBounds(self):
        first_slice = self.worker_slice_allocation[0]
        last_slice = self.worker_slice_allocation[-1]
        return ( first_slice[0], last_slice[1] )

class PVariable:

    def __init__(self, worker_comm, varName, dataset_path, time_specs, grid_specs, **args ):
        self.worker_comm = worker_comm
        self.global_comm = args.get( 'global_comm', MPI.COMM_WORLD )
        self.work_rank = worker_comm.Get_rank() if worker_comm else 0
        self.nprocs = self.global_comm.Get_size()
        self.nworkers = worker_comm.Get_size() if worker_comm else 1
        self.var_name = varName
        self.dataset_path = dataset_path
        self.time_specs = time_specs
        self.grid_specs = grid_specs 
        self.cdms_variables = {}

    @classmethod
    def recoverLostDim( cls, array, dim_index, original_rank ):  
        shape = list( array.shape )
        if original_rank > len( shape ):
            shape.insert( dim_index, 1 )
            return array.reshape( shape )
        else: return array
     
    @classmethod      
    def getBaseSizeAndShape( cls, shape, time_index ):
        prod = 1
        base_shape = [1]*len(shape)
        for index, sval in enumerate(shape):
            if index <> time_index: 
                prod = prod * sval
                base_shape[index] = sval
        return prod, base_shape
    

    def getAxes( self, slice_var, decomp, slices = None ):
        axes = []
        if slices == None: slices = decomp.slice_decomp_list 
        for iAxis in range( slice_var.rank() ):
            axis =  slice_var.getAxis( iAxis )
            if axis.isTime():
                time_data = []
                bounds = []                 
                for slice in slices:
                    if not isList( slice ): slice = [ slice ]
                    for timestamp in slice:
                        if isList( timestamp ):
                            rdt0 = TimeUtil.getRelTime( timestamp[0], decomp.period_units, decomp.global_start_time, axis.getCalendar() )
                            rdt1 = TimeUtil.getRelTime( timestamp[1], decomp.period_units, decomp.global_start_time, axis.getCalendar() )                           
                        else:
                            rdt0 = TimeUtil.getRelTime( timestamp, decomp.period_units, decomp.global_start_time, axis.getCalendar() )
                            rdt1 = rdt0.add( decomp.slice_length_value, decomp.slice_length_units )
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
    
    def dbg( self, msg ):
        print "PVar-%d: %s" % ( self.work_rank, str(msg) ); sys.stdout.flush()                  

    def execute( self, comp_kernel, gather = False ):
#        self.dbg( "Execute")
        tp0 = MPI.Wtime()
        ds = cdms2.open( self.dataset_path )
        self.var = ds[ self.var_name ]
        
        decomp = ParallelDecomposition( self.work_rank, self.nworkers )  
        decomp.processTimeMetadata( self.time_specs )
        decomp.generateTimeDecomposition()

                
        sel = self.getSelector( decomp )
        if sel: self.var = self.var(sel)
        tp1 = MPI.Wtime()
        tp = tp1 - tp0
        
        self.dbg("Finished IO, time = %.3f, Run Processing, Time Bounds: %s " % (  tp, str( decomp.getWorkerTimeBounds() ) ) )            
       
        tr0 = MPI.Wtime()
        self.runTimeProcessing( comp_kernel, decomp, gather )
        tr1 = MPI.Wtime()
        tr = tr1 - tr0

        self.dbg(" Task Completion: nslices = %d, IO time = %.3f sec, processing time = %.3f sec, total time = %.3f sec  " % ( len(self.cdms_variables), tp, tr, (tp+tr) ) )

        
    def runTimeProcessing( self, comp_kernel, decomp, gather ):
        self.timeAxisIndex = self.var.getAxisIndex('time')       
        
        results = []
        num_steps = decomp.getNumberOfSlices()
        
        result = None
        time_steps = []
        for iTime in range( num_steps ):
            ( t0, t1 ) = decomp.getSlice(iTime)
            slice_var = self.var( time=(t0,t1,'co')  )
            slice_array = numpy.ma.array( slice_var.data, mask=slice_var.mask )
             
            self.dbg("ProcessDataSlice[%d]: time = %s." % ( iTime, str(t0) ) )
            processedData = comp_kernel.execute( slice_array, axis=self.timeAxisIndex )  
            result = self.recoverLostDim( processedData, self.timeAxisIndex, self.var.rank() )
            
            if not isNone(result): 
                if gather:
                    results.append( result )
                    time_steps.append( t0 )
                else:
                    rvar = cdms2.createVariable( result, id=self.var.id, copy=0, axes=self.getAxes( slice_var, decomp, [ t0 ] ) )
                    self.cdms_variables[ t0.replace(' ','_').replace(':','-') ] = rvar 
       
        if gather:    
            merged_result = numpy.concatenate( results, self.timeAxisIndex ) 
            if self.worker_comm == None:
                rvar = cdms2.createVariable( merged_result, id=self.var.id, copy=0, axes=self.getAxes( slice_var, decomp ) ) 
            else:
                t0 = MPI.Wtime()
                gathered_result = self.gather( merged_result, decomp ) 
                t1 = MPI.Wtime()
                self.dbg("Gather time = %.2f, result shape = %s" % ( t1-t0, str(gathered_result.shape) )  )  
                rvar = cdms2.createVariable( gathered_result, id=self.var.id, copy=0, axes=self.getAxes( slice_var, decomp ) ) 
            self.cdms_variables[ time_steps[0].replace(' ','_').replace(':','-') ] = rvar 
                           

                                    
    def getLocalVariables(self):
        return self.cdms_variables
    
    def len(self):
        return len( self.cdms_variables )
                  
    def getSelector( self, decomp ):  
        sel = None
        lat_bounds = OpDomain.parseBoundsSpec( self.grid_specs.get( 'lat', None ) )
        if lat_bounds:
            if not isList( lat_bounds ): sel1 = Selector( latitude=lat_bounds )
            else: sel1 = Selector( latitude=lat_bounds[0] ) if ( len( lat_bounds ) == 1 ) else Selector( latitude=lat_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
        lon_bounds = OpDomain.parseBoundsSpec( self.grid_specs.get( 'lon', None ) )
        if lon_bounds:
            if not isList( lon_bounds ): sel1 = Selector( longitude=lon_bounds )
            else: sel1 = Selector( longitude=lon_bounds[0] ) if ( len( lon_bounds ) == 1 ) else Selector( longitude=lon_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
        lev_bounds = OpDomain.parseBoundsSpec( self.grid_specs.get( 'lev', None ) )
        if lev_bounds:
            if not isList( lev_bounds ): sel1 = Selector( level=lev_bounds )
            else: sel1 = Selector( level=lev_bounds[0] ) if ( len( lev_bounds ) == 1 ) else Selector( level=lev_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
        tsel = Selector( time=decomp.getWorkerTimeBounds() )
        sel= tsel if ( sel == None ) else sel & tsel    
        return sel
    
    def reduce( self, data_array ):
        return None
    
    def gather( self, data_array, decomp, proc_index = -1 ):
        new_shape = numpy.array( data_array.shape, dtype = 'f', copy=True )
        base_size, base_shape = self.getBaseSizeAndShape( data_array.shape, self.timeAxisIndex )
        nslices = decomp.nslice_map.sum()
#        self.dbg( "Gather: base_size = %s, base_shape = %s, nslice_map = %s, nslices = %s "  % ( str(base_size), str(base_shape), str(decomp.nslice_map), str(nslices) )  )    
        new_shape[ self.timeAxisIndex ] = nslices
        gathered_array = numpy.empty( [ new_shape.prod() ], dtype='f' )
        if proc_index < 0:
            self.worker_comm.Allgatherv( sendbuf=[ data_array.flatten(), MPI.FLOAT ], recvbuf=[ gathered_array, (decomp.nslice_map*base_size, None), MPI.FLOAT] )
        else: 
            self.worker_comm.Gatherv( sendbuf=[ data_array.flatten(), MPI.FLOAT ], recvbuf=[ gathered_array, (decomp.nslice_map*base_size, None), MPI.FLOAT], root=proc_index )
        result = None
        if (proc_index < 0) or ( proc_index == self.worker_comm.Get_rank() ):
            if self.timeAxisIndex == 0:
                result = gathered_array.reshape( new_shape )
            else:
                gathered_arrays = gathered_array.split( nslices )
                for array in gathered_arrays: array.reshape(base_shape) 
                result = numpy.concatenate( gathered_arrays, self.timeAxisIndex )
        return result
