'''
Created on Mar 28, 2014

@author: tpmaxwel
'''

'''
Created on Jan 14, 2014

@author: tpmaxwel
'''

import ClusterCommunicator
import os, sys, cdtime
from Utilities import *

class ConfigFileParser:
    
    def __init__ (self, configFilePath ):
        self.config_file = open( os.path.expanduser(configFilePath), 'r' )
        self.cats = multidict()
        self.current_cat = None
        self.current_cat_name = None
        self.parse()
        
    def parse(self):
        while True:
            line = self.config_file.readline()
            if not line: break
            else: line = line.strip()
            if line:
                if line[0] == '[': 
                    self.addCategory( line.strip('[] \t').lower() )
                else:
                    toks = line.split('=')
                    if len( toks ) == 2:
                        self.addField( toks[0].strip().lower(), toks[1].strip() )
                    
    def addCategory( self, cat_name ):
        self.current_cat = {}
        self.cats[ cat_name ] = self.current_cat
        self.current_cat_name = cat_name
            
    def addField( self, name, value ):
        if self.current_cat == None: self.addCategory( 'global' )
        self.current_cat[ name ] = value 
#        print "Add field: %s %s %s " % ( self.current_cat_name, name, value )
        
    def data(self): 
        return self.cats     

if __name__ == "__main__":
       
    app = ClusterCommunicator.getNodeApp( )
    
    short_run = False
    config_file = "./config/ParallelCDMS.nasa-monthly-sum-over-yearly-agg.txt"
    
    if config_file:
        config_parser = ConfigFileParser( config_file )
        task_metadata = config_parser.data()
        if short_run:
            time = task_metadata['time'] 
            start_time = cdtime.comptime( 1980, 0 )  
            end_time = cdtime.comptime( 1980, 2 )  
            time['start_time'] = str( start_time )   
            time['end_time'] = str( end_time )   
    else:    
    
        if short_run:
            start_time = cdtime.comptime( 1980, 0 )  
            end_time = cdtime.comptime( 1980, 7 )  
        else:
            start_time = cdtime.comptime( 1980, 1 )  
            end_time = cdtime.comptime( 1982, 1 ) 
        
        dataset = {}    
        dataset['path'] = '/Users/tpmaxwel/Data/MERRA_hourly_2D_precip/MERRA_hourly_precip_monthly_agg.xml'  #  MERRA_hourly_precip.xml'
        dataset[ 'variable' ] = 'prectot'    
    
        operation = {}    
        operation['domain'] = OpDomain.TIME
        operation['task'] =   'sum'   
        operation[ 'name' ] = 'MERRA_precip_monthly_totals'
    
        time = {} 
        time['start_time'] = str( start_time )   
        time['end_time'] = str( end_time )   
        time[ 'period_value' ] = 1   
        time[ 'period_units' ] = cdtime.Month  
    
        grid = {}    
    #    grid['lat'] = [ 40, 80 ]
        grid['lat'] = [ 0, 40 ]
        grid[ 'lon' ] = [ -180, 0 ]
        
        task_metadata = {}
        task_metadata['time'] = time
        task_metadata['grid'] = grid
        task_metadata['dataset'] = dataset
        task_metadata['operation'] = operation
        
    app.execute( task_metadata )
   
