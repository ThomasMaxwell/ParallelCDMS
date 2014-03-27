'''
Created on Mar 27, 2014

@author: tpmaxwel
'''
import cdutil, cdms2, os

class Task:
    
    def __init__(self, mdata ): 
        self.type = mdata.get( 'type', None )
        self.metadata = mdata
        
    def __getitem__(self, key):
        return self.metadata.get( key, None )
    
    def map( self, iproc, nprocs, metadata ):
        pass
        
    def reduce( self, data_array ):
        return None

class CDMSBlockTask(Task):
    
    def __init__(self, mdata ): 
        super( CDMSBlockTask, self ).__init__( mdata )    
            
    def map( self, iproc, nprocs, metadata ):
        dataset_path = self.metadata.get( 'dataset', None )
        var_name = self.metadata.get( 'variable', None )
        ds = cdms2.open( dataset_path )
        var = ds[ var_name ]
        cdutil.setTimeBoundsMonthly( var )
        ave = cdutil.JAN(var)
        data_dir = os.path.basename( dataset_path )
        outfile = cdms2.createDataset( os.path.join(data_dir,'ave_test.nc') )
        outfile.write( ave, index=0 )
        outfile.close()
        
    def reduce( self, data_array ):
        return None
