'''
Created on Apr 11, 2014

@author: tpmaxwel
'''
import os, sys, cdtime
from Utilities import *
from DistributedApplication import ConfigFileParser

class TaskCompiler:
    
    object_type_registry = {}

    @classmethod    
    def registerObjectType( cls, object_type, object_class ):
        cls.object_type_registry[ object_type ] = object_class
    
    def __init__(self, global_comm=None, task_comm=None, **args ):     
        self.global_comm = global_comm
        self.task_comm = task_comm
        self.rank = self.global_comm.Get_rank() if self.global_comm else 0 
        self.size = self.global_comm.Get_size() if self.global_comm else 1 
        self.worker_rank = self.task_comm.Get_rank() if self.task_comm else 0 
        self.objectstore = multidict()
        
    def getTaskObjects(self, object_type ):
        return getList( self.objectstore.get( object_type, None ) )
 
    def getTaskObject(self, object_type, name ):
        objs = self.objectstore.get( object_type, None )
        for obj in getList( objs ):  
            if obj and obj.name and ( obj.name.lower() == name.lower() ):
                return obj
        return None
            
    def processTaskSpec(self, task_spec ):
        for (obj_type,obj_specs) in task_spec.items():
            obj_class = self.object_type_registry.get( obj_type, None )
            if obj_class == None: 
                print>>sys.stderr, "Error, reference to unregistered object type: %s ", obj_type
            else:
                for obj_spec in getList( obj_specs ): 
                    self.objectstore[ obj_type ] = obj_class( obj_spec, self )                   
                
        dataset_objs = self.getTaskObjects( 'dataset')
        for dataset_obj in dataset_objs:
            ivars = dataset_obj.getSpecValues( 'vars' )  
            for ivar in ivars:
                object_spec = { 'name' : ivar, 'dataset' : dataset_obj.getSpecValue('path') }
                self.objectstore[ 'tvar' ] = TVarObject( object_spec, self )

        for objs in self.objectstore.values():
            for obj in getList( objs ):  
                obj.processObjects()
                
        output_objs = self.getTaskObjects( 'output')
        for output_obj in output_objs:
            ovars =  dataset_obj.getSpecValues( 'vars' )  
            for ovar_name in ovars:
                ovar = self.getTaskObject( 'tvar', ovar_name )   
                ovar.execute()
#        output_obj.execute()
        
        self.dump()
    
    def dump(self):       
        for item in self.objectstore.items():
            if isinstance( item[1], list ): 
                for spec in item[1]:
                    print "%s: %s" % ( str( item[0] ), str( spec ) )
            else:
                print "%s: %s" % ( str( item[0] ), str( item[1] ) )

class TaskObject:

    def __init__(self, object_spec, compiler ):
        self.spec = object_spec
        self.complier = compiler
        self.processSpec()
        
    def __str__(self):
        return " %s: %s " % ( self.__class__.__name__, str(self.spec) )
        
    def processSpec(self):  
        self.name = self.spec.get( 'name', None )
        
    def processObjects(self):
        pass
    
    def getSpecValues( self, key ):
        return getList( self.spec.get( key, None ) )

    def getSpecValue( self, key ):
        return self.spec.get( key, None ) 
        
    def getReferencedObjects(self, obj_type, **args ):
        singular=args.get( 'singular', False )
        obj_names = self.spec.get( obj_type, [] )
        if not isinstance( obj_names, list ): obj_names = [ obj_names ]
        objs = [ ]
        for obj_name in obj_names:
            obj = self.complier.getTaskObject( obj_type, obj_name )  
            if obj == None: 
                print>>sys.stderr, "Error, reference to undefined %s object: %s " % ( obj_type, obj_name )
            else:
                objs.append( obj )
        if len( objs ) == 0:
            return None
        else:
            if singular:
                if len( objs ) > 1: 
                    print>>sys.stderr, "Error, multiple definitions for singular object: %s " % ( obj_type )
                return objs[0]
            else:
                return objs
        
class TVarObject( TaskObject ):   
              
    def __init__(self, object_spec, compiler ): 
        TaskObject.__init__( self, object_spec, compiler ) 
        self.inputs = {}    

    def processSpec(self): 
        TaskObject.processSpec(self)
        
    def processObjects(self):   
        self.grid = self.getReferencedObjects( 'grid', singular=True )
        self.time_bounds = self.getReferencedObjects( 'time_bounds', singular=True )
        input_specs = self.getSpecValues( 'input' )
        for input_tvar_name in input_specs:
            input_tvar = self.complier.getTaskObject( 'tvar', input_tvar_name )
            self.inputs[input_tvar_name] = input_tvar 
            
    def execute(self):
        for input_object in self.inputs.values():
            input_object.execute()
        self.run_task()
        
    def run_task(self):
        pass 

    def __str__(self):
        return '\n'.join( [ TaskObject.__str__(self), "   -- grid: %s " % str( self.grid ), "   -- time_bounds: %s " % str( self.time_bounds ), "   -- inputs: %s " % str( self.inputs.items() ) ] )
        
TaskCompiler.registerObjectType( 'dataset', TaskObject )
TaskCompiler.registerObjectType( 'time_bounds', TaskObject )
TaskCompiler.registerObjectType( 'grid', TaskObject )
TaskCompiler.registerObjectType( 'tvar', TVarObject )
TaskCompiler.registerObjectType( 'output', TaskObject )

if __name__ == "__main__":
    
    configFilePath = "./config/ParallelCDMS.nasa-2.txt"
    cp = ConfigFileParser( configFilePath ) 
    cp.parse() 
        
    taskCompiler = TaskCompiler()
    taskCompiler.processTaskSpec( cp.data() )
