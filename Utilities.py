'''
Created on Jan 15, 2014

@author: tpmaxwel
'''

import numpy as np
import vtk, StringIO, cPickle, sys, os, cdtime, cdutil

class TimeUtil:
    time_str = [ "UNDEF", "seconds", "minutes", "hours", "days", "weeks", "months", "seasons", "years" ]    
    
    @classmethod
    def getRelTime( cls, ctime, units, ctime_base = None, calendar = cdtime.DefaultCalendar ):
        if isinstance( ctime, str ):        ctime = cls.getCompTime( ctime )
        if ctime_base == None: ctime_base = ctime
        rel_units = "%s since %s" % ( cls.time_str[units], str(ctime_base) )
        return ctime.torel( rel_units, calendar )
        
    
    @classmethod
    def getCDUnits( cls, str_units ):
        mstr = str_units[0:3].lower()
        for iUnit in range( len(cls.time_str) ):
            if mstr == cls.time_str[iUnit][0:3]:
                return iUnit
        return 0

    @classmethod
    def getCompTime( cls, str_time ):
        try:
            if str_time:
                itime = [ int( float(tok) ) for tok in str_time.replace('-',' ').replace(':',' ').split() ]
                return cdtime.comptime( *itime )
        except Exception, err:
            print>>sys.stderr,  "Error parsing time string '%s': %s" % ( str_time, str( err ) )
        return None 

class OpDomain:
    EXIT = 0
    TIME = 1
    SPACE = 2
    VARIABLES = 3

    
class TimeProcType:
    UNDEF = 0
    SUBSET = 1
    MEAN = 2
    ANOM = 3
    SUM = 3
    
def isNone( obj ):
    return id( obj ) == id( None )

def isList( val ):
    valtype = type(val)
    return ( valtype ==type(list()) ) or  ( valtype ==type(tuple()) )

def getItem( output, index = 0 ):  
    return output[ index ] if isList(output) else output  


def getMaxScalarValue( scalar_dtype ):
    if scalar_dtype == np.ushort:
        return 65535.0
    if scalar_dtype == np.ubyte:
        return 255.0 
    if scalar_dtype == np.float32:
        f = np.finfo(np.float32) 
        return f.max
    if scalar_dtype == np.float64:
        f = np.finfo(np.float64) 
        return f.max
    return None

def getNewVtkDataArray( scalar_dtype ):
    if scalar_dtype == np.ushort:
        return vtk.vtkUnsignedShortArray() 
    if scalar_dtype == np.ubyte:
        return vtk.vtkUnsignedCharArray() 
    if scalar_dtype == np.float32:
        return vtk.vtkFloatArray() 
    if scalar_dtype == np.float64:
        return vtk.vtkDoubleArray() 
    return None

def getDatatypeString( scalar_dtype ):
    if scalar_dtype == np.ushort:
        return 'UShort' 
    if scalar_dtype == np.ubyte:
        return 'UByte' 
    if scalar_dtype == np.float32:
        return 'Float' 
    if scalar_dtype == np.float64:
        return 'Double' 
    return None

def getStringDataArray( name, values = [] ):
    array = vtk.vtkStringArray()
    array.SetName( name )
    for value in values:
        array.InsertNextValue( value )
    return array

def encodeToString( obj ):
    rv = None
    try:
        buff = StringIO.StringIO()
        pickler = cPickle.Pickler( buff )
        pickler.dump( obj )
        rv = buff.getvalue()
        buff.close()
    except Exception, err:
        print>>sys.stderr, "Error pickling object %s: %s" % ( str(obj), str(err) )
    return rv

def newList( size, init_value ):
    return [ init_value for i in range(size) ]

def getHomeRelativePath( fullpath ):
    if not fullpath or not os.path.isabs( fullpath ): return fullpath
    homepath = os.path.expanduser('~')
    commonpath = os.path.commonprefix( [ homepath, fullpath ] )
    if (len(commonpath) > 1) and os.path.exists( commonpath ): 
        relpath = os.path.relpath( fullpath, homepath )
        return '/'.join( [ '~', relpath ] )
    return fullpath

def getFullPath( relPath ):
    return os.path.expanduser( relPath )

def isLevelAxis( axis ):
    if axis.isLevel(): return True
    if ( axis.id == 'isobaric' ): 
        axis.designateLevel(1)
        return True
    return False

def getVarNDim( vardata ):
    dims = [ 0, 0, 0 ]
    for dval in vardata.domain:
        axis = dval[0] 
        if axis.isLongitude(): 
            dims[0] = 1
        elif axis.isLatitude(): 
            dims[1] = 1
        elif isLevelAxis( axis ): 
            dims[2] = 1
    return dims[0] + dims[1] + dims[2]
