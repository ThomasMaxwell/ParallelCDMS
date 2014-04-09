'''
Created on Apr 9, 2014

@author: tpmaxwel
'''
import cdtime, cdms2, time
from mpi4py import MPI
from PVariable import ParallelDecomposition, DecompositionMode
from Utilities import *
from cdms2.selectors import Selector

class IOTestApp:
        
    def __init__(self, task_metadata):
        self.task_metadata = task_metadata
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.nprocs = self.comm.Get_size()
        dataset_metadata = self.task_metadata[ 'dataset' ]
        self.dataset_path = dataset_metadata.get( 'path', None )
        self.time_specs = self.task_metadata[ 'time' ]
        self.grid_specs = self.task_metadata[ 'grid' ]        
        self.var_name = dataset_metadata.get( 'variable', None )
        self.decomp = ParallelDecomposition( self.rank, self.nprocs )  
        
    def execute( self, decomp_mode ):
        tp0 = time.clock()
        
        self.decomp.generateDecomposition( decomp_mode, self.task_metadata )

        
        ds = cdms2.open( self.dataset_path )
        self.var = ds[ self.var_name ] 
        sel = self.getSelector( self.decomp )
        if sel: self.var = self.var(sel)
        
        tp1 = time.clock()
        tp = tp1 - tp0
        
        print "Variable Read completed in %.2f sec" % tp

    def getSelector( self, decomp ):  
        sel = None
        decomp_mode = decomp.getDecompositionMode()
        
        lat_bounds = OpDomain.parseBoundsSpec( self.grid_specs.get( 'lat', None ) )
        if lat_bounds:
            if not isList( lat_bounds ): sel1 = Selector( latitude=lat_bounds )
            else: sel1 = Selector( latitude=lat_bounds[0] ) if ( len( lat_bounds ) == 1 ) else Selector( latitude=lat_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
         
        lon_bounds=decomp.getWorkerSpaceBounds() if (decomp_mode == DecompositionMode.Spatial) else OpDomain.parseBoundsSpec( self.grid_specs.get( 'lon', None ) )
        if lon_bounds:
            if not isList( lon_bounds ): sel1 = Selector( longitude=lon_bounds )
            else: sel1 = Selector( longitude=lon_bounds[0] ) if ( len( lon_bounds ) == 1 ) else Selector( longitude=lon_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
            print "P-%d: Setting Lon Bounds: %s " % ( self.rank, str( lon_bounds ) )  
                    
        lev_bounds = OpDomain.parseBoundsSpec( self.grid_specs.get( 'lev', None ) )
        if lev_bounds:
            if not isList( lev_bounds ): sel1 = Selector( level=lev_bounds )
            else: sel1 = Selector( level=lev_bounds[0] ) if ( len( lev_bounds ) == 1 ) else Selector( level=lev_bounds )
            sel= sel1 if ( sel == None ) else sel & sel1
            
        time_bounds = decomp.getWorkerTimeBounds() if decomp_mode == DecompositionMode.Temporal else decomp.getGlobalTimeBounds()
        tsel = Selector( time=time_bounds )
        print "P-%d: Setting Time Bounds: %s " % ( self.rank, str( time_bounds ) )          
        sel= tsel if ( sel == None ) else sel & tsel 
        return sel

if __name__ == "__main__":
       
    short_run = True
    start_time = cdtime.comptime( 1980, 1 )  
    end_time = cdtime.comptime( 1980, 7 ) if short_run else cdtime.comptime( 1982, 1 ) 
    decomp_mode = DecompositionMode.Temporal
    
    dataset = {}    
    dataset['path'] = '/Users/tpmaxwel/Data/MERRA_hourly_2D_precip/MERRA_hourly_precip.xml'
    dataset[ 'variable' ] = 'prectot'    

    time_specs = {} 
    time_specs['start_time'] = str( start_time )   
    time_specs['end_time'] = str( end_time )   
    time_specs[ 'period_value' ] = 1   
    time_specs[ 'period_units' ] = cdtime.Month  

    grid = {}    
    grid['lat'] = [ -90, 90 ]
    grid[ 'lon' ] = [ -180, 180 ]
    
    task_metadata = {}
    task_metadata['time'] = time_specs
    task_metadata['grid'] = grid
    task_metadata['dataset'] = dataset
        
    testApp = IOTestApp( task_metadata )
    testApp.execute( decomp_mode )
