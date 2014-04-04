'''
Created on Mar 31, 2014

@author: tpmaxwel
'''

import cdtime, numpy, sys, copy

def serializeSpec( spec ):
    if isinstance( spec, list ) or isinstance( spec, tuple ):
        return [ str(item) for item in spec ]
    else: return str( spec )

class TaskMapper:

    def __init__(self, nprocs, **args ):
        self.nprocs = nprocs
                   
    def getTimeDecomposition( self, start_time, end_time, slice_period, slice_length = None ):
        t0 = start_time 
        time_slices = []
        while True:
            if slice_length == None:
                time_slices.append( t0 )
                t1 = t0
            else:
                t1 = t0.add( *slice_length )
                if t1.cmp( end_time ) > 0: break   
                time_slices.append( ( t0, t1 ) )
            
            t0 = t0.add( *slice_period )
            if t0.cmp( end_time ) > 0: break                 

        nslices = len( time_slices ) - 1
        n_worker_procs = self.nprocs - 1 if (self.nprocs > 1) else 1
        if nslices > n_worker_procs:
            nexcess_slices = nslices / n_worker_procs 
            nexcess_slices_rem = nslices - nexcess_slices*n_worker_procs - 1
            nslice_map = [ nexcess_slices if ( iSlice >= nexcess_slices_rem ) else nexcess_slices + 1 for iSlice in range(n_worker_procs) ]
        else:
            nslice_map = [ 1 ] * nslices
        
        decomp = [ ] 
        slice_index = 0  
        for iProc in range( len( nslice_map ) ):
            try:
                nslices = nslice_map[ iProc ] 
                slice_spec_start = time_slices[ slice_index ]
                time_list = [ serializeSpec( slice_spec_start ) ]
                n_additional_slices = nslices if (slice_length == None) else nslices-1
                for iSlice in range( n_additional_slices ):
                    slice_spec = time_slices[ slice_index + iSlice + 1 ]
                    time_list.append( serializeSpec( slice_spec ) )
                decomp.append( [ time_list, slice_index ] )
                slice_index = slice_index + nslices
            except Exception, err:
                print str(err)
                            
        return decomp

if __name__ == "__main__":
    test_number = 0
    
    tm = TaskMapper( 3 )
    
    if test_number == 0:
        start_time = cdtime.comptime( 1980, 1 )  
        end_time = cdtime.comptime( 1981, 1 ) 
        slice_period = ( 1, cdtime.Month )        
        decomp = tm.getTimeDecomposition( start_time, end_time, slice_period )
        
        print " nslices = %d: decomp = %s, " % ( len(decomp), str( decomp ) )   
#        print " nslice_map = ", str( nslice_map )
 #       print " time_slices = ", str( time_slices )
        
    if test_number == 1:
        start_time = cdtime.comptime( 1996, 3 ) 
        end_time = cdtime.comptime( 2001, 6 )     
        slice_period = ( 1, cdtime.Year )        
        slice_length = ( 3, cdtime.Month )        
        decomp = tm.getTimeDecomposition( start_time, end_time, slice_period, slice_length )
        
        print " nslices = %d: decomp = %s, " % ( len(decomp), str( decomp ) )   
#        print " nslice_map = ", str( nslice_map )
#        print " time_slices = ", str( time_slices )
        
        
        
        
        
    
    
    
    
    
    