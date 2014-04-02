'''
Created on Apr 2, 2014

@author: tpmaxwel
'''
import numpy, cdutil, cdtime, sys, copy

# class TimeProcPeriod:
#     JAN = 0
#     FEB = 1
#     MAR = 2
#     APR = 3
#     MAY = 4
#     JUN = 5
#     JUL = 6
#     AUG = 7
#     SEP = 8
#     OCT = 9
#     NOV = 10
#     DEC = 11
#     MONTHLY = 19
#     DJF = 20
#     MAM = 21
#     JJA = 22
#     SON = 23
#     YEAR = 24
#     ANNUAL = 25
#     SEASONAL = 26
#     
#     opMap = { JAN: cdutil.JAN, FEB: cdutil.FEB, MAR: cdutil.MAR, APR: cdutil.APR, MAY: cdutil.MAY, JUN: cdutil.JUN, 
#              JUL: cdutil.JUL, AUG: cdutil.AUG, SEP: cdutil.SEP, OCT: cdutil.OCT, NOV: cdutil.NOV, DEC: cdutil.DEC,
#               DJF: cdutil.DJF, MAM: cdutil.MAM, JJA: cdutil.JJA, SON: cdutil.SON, YEAR: cdutil.YEAR,  MONTHLY: cdutil.ANNUALCYCLE,  SEASONAL: cdutil.SEASONALCYCLE }
#     
#     @classmethod
#     def getAverager( cls, period ):
#         return cls.opMap.get( period, None )

#     @classmethod
#     def execute( cls, averager, opType, var ):
#         if opType == cls.SUBSET: return averager(var)
#         if opType == cls.MEAN:   return averager.climatology(var)
#         if opType == cls.ANOM:   return averager.departures(var)
#         return None

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
