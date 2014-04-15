'''
Created on Apr 4, 2014

@author: tpmaxwel
'''


import ClusterCommunicator
from DistributedApplication import ConfigFileParser
import sys
         
app = ClusterCommunicator.getNodeApp( )
task_metadata = {}
config_file = "./config/ParallelCDMS.nasa-desktop-merge.txt"

if len(sys.argv)>2 and sys.argv[1] == '-c':
    config_file = sys.argv[2] 
    
print "Running PCDMS with config file ", config_file  
config_parser = ConfigFileParser( config_file )
task_metadata = config_parser.data()     
        
app.execute( task_metadata )
   

