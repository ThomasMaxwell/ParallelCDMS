'''
Created on Apr 4, 2014

@author: tpmaxwel
'''


import ClusterCommunicator
from DistributedApplication import ConfigFileParser
import sys
         
app = ClusterCommunicator.getNodeApp( )
task_metadata = {}
    
if len(sys.argv)>2 and sys.argv[1] == '-c':
    config_parser = ConfigFileParser( sys.argv[2] )
    task_metadata = config_parser.data()

        
app.execute( task_metadata )
   

