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
    
    def map( self, iproc, nprocs, local_metadata={} ):
        pass
        
    def reduce( self, data_array ):
        return None

class CDMSBlockTask(Task):
    
    def __init__(self, mdata ): 
        Task.__init__( self, mdata )    
            
    def map( self, iproc, nprocs, local_metadata={} ):
        dataset_path = self.metadata.get( 'dataset', None )
        var_name = self.metadata.get( 'variable', None )
        ds = cdms2.open( dataset_path )
        var = ds[ var_name ]
        cdutil.times.setSlabTimeBoundsDaily( var, frequency=8 )
        ave = cdutil.JAN(var)
        data_dir = os.path.basename( dataset_path )
        outfile = cdms2.createDataset( os.path.join(data_dir,'ave_test.nc') )
        outfile.write( ave, index=0 )
        outfile.close()
        
    def reduce( self, data_array ):
        return None


if __name__ == "__main__":
    
    metadata = {}
    metadata[ 'dataset' ] = '/Users/tpmaxwel/Data/MERRA/DAILY/2005/JAN/merra_daily_T_JAN_2005.xml'
    metadata[ 'variable' ] = 't'
    
    task = CDMSBlockTask( metadata )
    task.map( 0, 1 )