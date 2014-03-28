'''
Created on Mar 28, 2014

@author: tpmaxwel
'''

'''
Created on Jan 14, 2014

@author: tpmaxwel
'''

import ClusterCommunicator
import os, sys
from Utilities import isList

class ConfigFileParser:
    
    def __init__ (self, configFilePath ):
        self.config_file = open( os.path.expanduser(configFilePath), 'r' )
        self.cats = {}
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
        if cat_name in self.cats:
            self.current_cat = self.cats[ cat_name ]
        else:
            self.current_cat = {}
            self.cats[ cat_name ] = self.current_cat
        self.current_cat_name = cat_name
            
    def addField( self, name, value ):
        if self.current_cat == None: self.addCategory( 'global' )
#        vlist = value.split(',')
        self.current_cat[ name ] = value 
#        print "Add field: %s %s %s " % ( self.current_cat_name, name, value )
        
    def data(self): 
        return self.cats     

if __name__ == "__main__":

    app = ClusterCommunicator.getNodeApp()
    
    if app.rank == 0:
        if len(sys.argv)>2 and sys.argv[1] == '-c':
            config = ConfigFileParser( sys.argv[2] )
            app.processConfigData( config.data() )

    else:
        pass     
