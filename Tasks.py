'''
Created on Mar 27, 2014

@author: tpmaxwel
'''
import cdutil, cdms2, os, numpy, copy, time
from cdms2.selectors import Selector
from Utilities import *

class TimeMgr:
    time_str = [ "UNDEF", "seconds", "minutes", "hours", "days", "weeks", "months", "seasons", "years" ]

    @classmethod
    def compToRel( cls, comptime, units ):
        reltime_units = "%s since %s" % ( cls.time_str[units], str(comptime) )
        return comptime.torel( reltime_units )
    
    @classmethod
    def getCDUnits( cls, str_units ):
        mstr = str_units[0:3].lower()
        for iUnit in range( len(cls.time_str) ):
            if mstr == cls.time_str[iUnit][0:3]:
                return iUnit
        return 0

    @classmethod
    def setAxisTimeBoundsMonthly(cls, axis ):
        """ Sets the bounds correctly for the time axis (beginning to end of month)
        Usage:
        tim=s.getTime()
        cdutil.times.setAxisTimeBoundsMonthly(tim,stored=0)
        """
        tim=axis
        if not tim.isTime():
            raise ValueError,'Time Axis only please !'
        if tim is None:
            return
        units=tim.units
        timc=tim.asComponentTime()
        n=len(tim)
        bnds=numpy.zeros((n,2),numpy.float)
        for i in range(n):
            t=timc[i]
            d=t.day
            m=t.month
            y=t.year
            t1=cdtime.comptime(y,m)
            t2=t1.add(1,cdtime.Month,tim.getCalendar())
            t1=t1.torel(units,tim.getCalendar())
            t2=t2.torel(units,tim.getCalendar())
            bnds[i,0]=t1.value
            bnds[i,1]=t2.value
        cls._setTimeBounds(tim,bnds)
        return
    
    @classmethod
    def setAxisTimeBoundsDaily(cls, axis,frequency=1):
        """ Sets the bounds correctly for the time axis (beginning to end of day)
        Usage:
        tim=s.getTime()
        cdutil.times.setAxisTimeBoundsMonthly(tim,frequency=1)
        e.g. for twice-daily data use frequency=2
             for 6 hourly data use frequency=4
             for   hourly data use frequency=24
        Origin of day is always midnight
        """
        tim=axis
        if not tim.isTime():
            raise ValueError,'Time Axis only please !'
        if tim is None:
            return
        units=tim.units
        timc=tim.asComponentTime()
        n=len(tim)
        bnds=numpy.zeros((n,2),numpy.float)
        frequency=int(frequency)
        for i in range(n):
            t=timc[i]
            d=t.day
            m=t.month
            y=t.year
            h=t.hour
            for f in range(frequency):
                if f*(24/frequency)<=h<(f+1)*(24/frequency):
                    t1=cdtime.comptime(y,m,d,f*(24/frequency))
                    t2=t1.add(24/frequency,cdtime.Hours,tim.getCalendar())
                    t1=t1.torel(units,tim.getCalendar())
                    t2=t2.torel(units,tim.getCalendar())
            bnds[i,0]=t1.value
            bnds[i,1]=t2.value
        cls._setTimeBounds(tim,bnds)
        return

    @classmethod
    def _setTimeBounds( cls, axis, bounds ):
        if isinstance(bounds, numpy.ma.MaskedArray):
            bounds = numpy.ma.filled(bounds)                   
        requiredShape = (len(axis),2)
        requiredShape2 = (len(axis)+1,)
        if bounds.shape!=requiredShape and bounds.shape!=requiredShape2:
            print>>sys.stderr, ' InvalidBoundsArray: shape is %s, should be %s or %s' % ( bounds.shape, requiredShape, requiredShape2 )
        if bounds.shape==requiredShape2: 
            bounds2=numpy.zeros(requiredShape)
            bounds2[:,0]=bounds[:-1]
            bounds2[:,1]=bounds[1::]
            bounds=bounds2
        axis._bounds_ = copy.copy(bounds)
     
    @classmethod   
    def setTimeBounds( cls, axis, frequency, time_units ):
        if ( time_units == cdtime.Month ) or ( time_units == cdtime.Months ):
            cls.setAxisTimeBoundsMonthly( axis )
        elif ( time_units == cdtime.Day ) or ( time_units == cdtime.Days ):
            cls.setAxisTimeBoundsDaily( axis, frequency )
        elif ( time_units == cdtime.Hour ) or ( time_units == cdtime.Hours ):
            cls.setAxisTimeBoundsDaily( axis, frequency*24 )

class AnalysisClass:
    UNDEF = 0
    TEMPORAL_MEAN = 1
    SPATIAL_MEAN = 2
    
class TimeProcPeriod:
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
    MONTHLY = 19
    DJF = 20
    MAM = 21
    JJA = 22
    SON = 23
    YEAR = 24
    ANNUAL = 25
    SEASONAL = 26
    
    opMap = { JAN: cdutil.JAN, FEB: cdutil.FEB, MAR: cdutil.MAR, APR: cdutil.APR, MAY: cdutil.MAY, JUN: cdutil.JUN, 
             JUL: cdutil.JUL, AUG: cdutil.AUG, SEP: cdutil.SEP, OCT: cdutil.OCT, NOV: cdutil.NOV, DEC: cdutil.DEC,
              DJF: cdutil.DJF, MAM: cdutil.MAM, JJA: cdutil.JJA, SON: cdutil.SON, YEAR: cdutil.YEAR,  MONTHLY: cdutil.ANNUALCYCLE,  SEASONAL: cdutil.SEASONALCYCLE }
    
    @classmethod
    def getAverager( cls, period ):
        return cls.opMap.get( period, None )
    
class TimeProcType:
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
        [ self.time_list, self.num_steps ] = time_slab
#         self.start_time = getCompTime(time0)
#         self.end_time   = getCompTime(time1)

    def map( self, iproc, nprocs ):
        dataset_metadata = self.task_metadata[ 'dataset' ]
        dataset_path = dataset_metadata.get( 'path', None )
        result_name = dataset_metadata.get( 'result_name', "TemporalProcessingResult.nc" )
        var_name = dataset_metadata.get( 'variable', None )
        time_units = dataset_metadata.get( 'time_units', None )
        time_frequency = dataset_metadata.get( 'time_frequency', None )
        ds = cdms2.open( dataset_path )
        var = ds[ var_name ]
        sel = self.getSelector()
        if sel: var = var(sel)
        
#         time_axis = var.getTime()
#         TimeMgr.setTimeBounds( time_axis, time_frequency, time_units )
        
        t0 = time.clock()
        results = self.runTimeProcessing( var )
        t1 = time.clock()

        print " Computed result, shape = %s, processing time = %.3f sec " % ( str( results[0].shape ), (t1-t0) )
#         data_dir = os.path.basename( dataset_path )
#         outfile = cdms2.createDataset( os.path.join( data_dir, result_name ) )
#         outfile.write( result, index=0 )
#         outfile.close()

            
    def runTimeProcessing( self, var ):
        operation_metadata = self.task_metadata[ 'operation' ]
        opType = operation_metadata.get( 'type', TimeProcType.UNDEF ) 
        timeAxisIndex = var.getAxisIndex('time')       
        
        print "Running time processing."
        results = []
        
        for iTime in range( self.num_steps ):
            t0 = self.time_list[iTime]
            t1 = self.time_list[iTime+1]        
            result = numpy.sum( var( time=(t0,t1,'co') ), axis = timeAxisIndex  )
            results.append( result )
    
#         averager = TimeProcPeriod.getAverager( op_period )
#         result = TimeProcType.execute(averager, opType, var )
        
        return results
       
        
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
        tsel = Selector( time=( self.time_list[0], self.time_list[-1] ) )
        sel= tsel if ( sel == None ) else sel & tsel    
        return sel
    
#     def map1( self, iproc, nprocs, local_metadata={} ):
#         dataset_path = self.metadata.get( 'dataset', None )
#         var_name = self.metadata.get( 'variable', None )
#         ds = cdms2.open( dataset_path )
#         var = ds[ var_name ]
#         print var.info()
#         sel = self.getSelector()
#         if sel: var = var[sel]
#         
#         analysisClass = self.metadata.get( 'class', AnalysisClass.UNDEF )
#         opType = self.metadata.get( 'type', TimeAveOpType.UNDEF )
#         
#         if analysisClass == AnalysisClass.TEMPORAL_MEAN:
#             opPeriod = self.metadata.get( 'period', TimeAveOpPeriod.UNDEF )
#             averager = TimeAveOpPeriod.getAverager( opPeriod )
#             result = TimeAveOpType.execute( averager, opType, var )
#             
#             data_dir = os.path.basename( dataset_path )
#             outfile = cdms2.createDataset( os.path.join(data_dir,'ave_test.nc') )
#             outfile.write( result, index=0 )
#             outfile.close()




#        cdutil.times.setSlabTimeBoundsDaily( var, frequency=8 )
        
    def reduce( self, data_array ):
        return None


def getTask( task_spec ):
    task_domain = task_spec[0].lower()
    if task_domain == 'quit': return None
    if task_domain == 'temporal': 
        return TemporalProcessing( task_spec[1:] )   
    
if __name__ == "__main__":

    time0 = cdtime.comptime( 1980, 1 ) 
    time1 = cdtime.comptime( 1980, 2 ) 
    time2 = cdtime.comptime( 1980, 3 ) 
    
    num_steps = 2     
    task_spec = {}
    
    dataset = {}    
    dataset['path'] = '/Users/tpmaxwel/Data/MERRA_hourly_2D_precip/MERRA_hourly_precip.xml'
    dataset[ 'variable' ] = 'prectot'    
    dataset[ 'time_units' ] = cdtime.Hour  
    dataset[ 'time_frequency' ] = 1    
    task_spec[ 'dataset' ] = dataset

    operation = {}    
    operation['type'] = TimeProcType.MEAN  
    operation[ 'period' ] = TimeProcPeriod.MONTHLY   
    operation[ 'result_name' ] = 'TemperatureAverageTest.nc'
    task_spec[ 'operation' ] = operation

    grid = {}    
    grid['lat'] = [ 40, 80 ]
    grid[ 'lon' ] = [ -180, 0 ]
    task_spec[ 'grid' ] = grid
    
    time_slab = [ ( str(time0), str(time1), str(time2) ), num_steps ]
    
    task = TemporalProcessing( ( time_slab, task_spec ) )
    task.map( 0, 1 )