'''
Created on Mar 31, 2014

@author: tpmaxwel
'''

import cdtime, numpy, sys, copy

class TaskMapper:

    def __init__(self, nprocs, **args ):
        self.nprocs = nprocs
                   
    def getTimeDecomposition( self, start_time, end_time, period, units ):
        t0 = start_time 
        time_slices = [ t0 ]
        while True:
            t1 = t0.add( period, units )
            time_slices.append( t1 )
            if t1.cmp( end_time ) >= 0: break
            t0 = t1
        nslices = len( time_slices )
        if nslices > self.nprocs:
            nexcess_slices = nslices / self.nprocs 
            nexcess_slices_rem = nslices - nexcess_slices*self.nprocs - 1
            nslice_map = [ nexcess_slices if ( iSlice >= nexcess_slices_rem ) else nexcess_slices + 1 for iSlice in range(self.nprocs) ]
        else:
            nslice_map = [ 1 ] * nslices
        
        decomp = [ ] 
        slice_index = 0  
        for iProc in range( len( nslice_map ) ):
            try:
                nslices = nslice_map[ iProc ]
                ts_start = time_slices[ slice_index ]
                time_list = [ str(ts_start) ]
                for iSlice in range( nslices ):
                    ts1 = time_slices[ slice_index + iSlice + 1 ]
                    time_list.append( str(ts1) )
                decomp.append( [ time_list, nslices ] )
                slice_index = slice_index + nslices
            except Exception, err:
                print str(err)
                            
        return decomp, nslice_map, time_slices

if __name__ == "__main__":
    
    tm = TaskMapper( 10 )
    
    start_time = cdtime.comptime( 1996, 2 ) 
    end_time = cdtime.comptime( 1997, 2 )  
    period = 1 
    units = cdtime.Month
       
    decomp, nslice_map, time_slices = tm.getTimeDecomposition( start_time, end_time, period, units )
    
    print " nslices = %d: decomp = %s, " % ( len(decomp), str( decomp ) )   
    print " nslice_map = ", str( nslice_map )
    print " time_slices = ", str( time_slices )
    
    
    
    
    
    
    
    
    
    
    
    