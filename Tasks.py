'''
Created on Mar 27, 2014

@author: tpmaxwel
'''
import cdutil, cdms2, os
from cdms2.selectors import Selector
from Utilities import *

class AnalysisClass:
    UNDEF = 0
    TEMPORAL_MEAN = 1
    SPATIAL_MEAN = 2
    
class TimeAveOpPeriod:
    JAN = 0
    FEB = 1
    MAR = 2
    APR = 3
    MAY = 4
    JUN = 5
    JUL = 6
    AUG = 7
    SEP = 8
    OCT = 9
    NOV = 10
    DEC = 11
    DJF = 20
    MAM = 21
    JJA = 22
    SON = 23
    YEAR = 24
    ANNUAL = 25
    SEASONAL = 26
    
    opMap = { JAN: cdutil.JAN, FEB: cdutil.FEB, MAR: cdutil.MAR, APR: cdutil.APR, MAY: cdutil.MAY, JUN: cdutil.JUN, 
             JUL: cdutil.JUL, AUG: cdutil.AUG, SEP: cdutil.SEP, OCT: cdutil.OCT, NOV: cdutil.NOV, DEC: cdutil.DEC,
              DJF: cdutil.DJF, MAM: cdutil.MAM, JJA: cdutil.JJA, SON: cdutil.SON, YEAR: cdutil.YEAR,  ANNUAL: cdutil.ANNUALCYCLE,  SEASONAL: cdutil.SEASONALCYCLE }
    
    @classmethod
    def getAverager( cls, period ):
        return cls.opMap.get( period, None )
    
class TimeAveOpType:
    UNDEF = 0
    SUBSET = 1
    MEAN = 2
    ANOM = 3
    
    @classmethod
    def execute( cls, averager, opType, var ):
        if opType == cls.SUBSET: return averager(var)
        if opType == cls.MEAN:   return averager.climatology(var)
        if opType == cls.ANOM:   return averager.departures(var)
        return None

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
        ( time_slab, self.task_metadata ) = task_spec            
        [ ( time0, time1 ), self.num_steps ] = time_slab
        self.start_time = getCompTime(time0)
        self.end_time   = getCompTime(time1)

    def map( self, iproc, nprocs ):
        dataset_metadata = self.task_metadata[ 'dataset' ]
        dataset_path = dataset_metadata.get( 'dataset', None )
        var_name = dataset_metadata.get( 'variable', None )
        ds = cdms2.open( dataset_path )
        var = ds[ var_name ]
        print var.info()
        sel = self.getSelector()
        if sel: var = var[sel]
        
        operation_metadata = self.task_metadata[ 'operation' ]
        opType = operation_metadata.get( 'type', TimeAveOpType.UNDEF )

        data_dir = os.path.basename( dataset_path )
        outfile = cdms2.createDataset( os.path.join(data_dir,'ave_test.nc') )
        outfile.write( result, index=0 )
        outfile.close()
        
    def getSelector(self):  
        sel = None
        grid_metadata = self.task_metadata[ 'grid' ]
        lat_bounds = grid_metadata.get( 'lat', None )
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
        tsel = Selector( time=( str(self.start_time), str(self.end_time) ) )
        sel= tsel if ( sel == None ) else sel & tsel    
        return sel
    
    def map1( self, iproc, nprocs, local_metadata={} ):
        dataset_path = self.metadata.get( 'dataset', None )
        var_name = self.metadata.get( 'variable', None )
        ds = cdms2.open( dataset_path )
        var = ds[ var_name ]
        print var.info()
        sel = self.getSelector()
        if sel: var = var[sel]
        
        analysisClass = self.metadata.get( 'class', AnalysisClass.UNDEF )
        opType = self.metadata.get( 'type', TimeAveOpType.UNDEF )
        
        if analysisClass == AnalysisClass.TEMPORAL_MEAN:
            opPeriod = self.metadata.get( 'period', TimeAveOpPeriod.UNDEF )
            averager = TimeAveOpPeriod.getAverager( opPeriod )
            result = TimeAveOpType.execute( averager, opType, var )
            
            data_dir = os.path.basename( dataset_path )
            outfile = cdms2.createDataset( os.path.join(data_dir,'ave_test.nc') )
            outfile.write( result, index=0 )
            outfile.close()




#        cdutil.times.setSlabTimeBoundsDaily( var, frequency=8 )
        
    def reduce( self, data_array ):
        return None


def getTask( task_spec ):
    task_domain = task_spec[0].lower()
    if task_domain == 'quit': return None
    if task_domain == 'temporal': 
        return TemporalProcessing( task_spec[1:] )   
    
if __name__ == "__main__":
    
    metadata = {}
    metadata[ 'dataset' ] = '/Users/tpmaxwel/Data/MERRA/DAILY/2005/JAN/merra_daily_T_JAN_2005.xml'
    metadata[ 'variable' ] = 't'
    
    task = CDMSBlockTask( metadata )
    task.map( 0, 1 )