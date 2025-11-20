__author__ = 'Pablo Ramos Criado'
__students__ = 'Santiago Garcia Dominguez & Fernando Contreras Ramirez'


from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
from typing import Generator, Any, Self
from geojson import Point
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
import yaml
from dotenv import load_dotenv
import redis
import os
import json
import random
from sesiones import Sesiones

load_dotenv()

REDIS_PASSWORD = os.getenv("REDIS_PSSWD")
REDIS_UNAME = os.getenv("REDIS_UNAME")
REDIS_HOST = os.getenv("REDIS_HOST")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
DEFINITIONS_PATH = os.getenv("DEF_PATH")


BBDD_REDIS = {
    "cache": 0,
    "sesiones": 1,
}

# declaramos las conexiones con un bucle k-v 
redis_conns = {
    name: redis.Redis(host= REDIS_HOST, port = 11207, db = dbnum, username = REDIS_UNAME, password= REDIS_PASSWORD, decode_responses = True)
    for name, dbnum in BBDD_REDIS.items()
}

CACHE: dict[str, Point | str] = {} #clave string valor Point
FAIL_MESSAGE = "No se pudieron obtener coordenadas"
NOT_ADMITTED_VARIABLE = "No esta permitida usar esta variable"

def getLocationPoint(address: str) -> Point:

    if not address:
        raise ValueError(FAIL_MESSAGE)
    
    if address in CACHE:
        return CACHE[address] 
    
    location = None
    attempts = 0

    while location is None:
        try:
            time.sleep(2)
            location = Nominatim(user_agent="santifer").geocode(address)
        except GeocoderTimedOut:
            attempts +=1
            if attempts >= 3:
                raise ValueError(FAIL_MESSAGE)
            continue

    point = Point((location.longitude, location.latitude))
    CACHE[address] = point
    return point


class Model:
    
    _required_vars: set[str]
    _admissible_vars: set[str]
    _location_var: None
    _db: pymongo.collection.Collection
    _internal_vars: set[str]={}
    _redis = None

    def __init__(self, **kwargs: dict[str, str | dict]):
        
        self._data: dict[str, str | dict] = {}
        
        super().__setattr__("_data", {}) 
        super().__setattr__("_modified_vars", set()) 

        for campo_requerido in self._required_vars:
              if campo_requerido not in kwargs:
                raise ValueError(f"El atributo requerido '{campo_requerido}' es obligatorio y no se ha proporcionado.")
        
        
        for atributo_perimitido in kwargs:
            if atributo_perimitido not in self._admissible_vars:
                raise ValueError(f"El atributo requerido '{atributo_perimitido}'no es admisible.")
        
    
        self._data.update(kwargs)


    def __setattr__(self, name: str, value: str | dict) -> None:

        if name in {'_modified_vars', '_required_vars', '_admissible_vars', '_db', '_location_var', '_data'}:
            super().__setattr__(name, value)
            return

        
        if name not in self._admissible_vars:
            
            raise AttributeError(f"El atributo '{name}' no es admitido por el modelo.")


        self._data[name] = value
        
        self._modified_vars.add(name)

        
        if name == self._location_var:
            location_point = getLocationPoint(value)
            if location_point is None:
                self._data[f"{self._location_var}_loc"] = FAIL_MESSAGE
            else:
                self._data[f"{self._location_var}_loc"] = location_point


    def __getattr__(self, name: str) -> Any:
        
        if name in {'_modified_vars', '_required_vars', '_admissible_vars', '_db', '_data', '_location_var'}:
            return super().__getattribute__(name)
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError
        
    def save(self) -> None:

        
        if "_id" in self._data:
        
            update_doc = {k: self._data[k] for k in getattr(self, "_modified_vars", set())}
            update_doc.pop("_id", None)
            if update_doc:
                self._db.update_one({"_id": self._data["_id"]}, {"$set": update_doc})
                self._modified_vars.clear()
        else:
        
            result = self._db.insert_one(dict(self._data))
            self._data["_id"] = result.inserted_id
            self._modified_vars.clear()



    def delete(self) -> None:
        
        if "_id" in self._data:
            self._db.delete_one({"_id": self._data["_id"]})
            
            self._data.clear()
            self._modified_vars.clear()
        else:
            raise ValueError("El modelo no existe en la base de datos.")
    
    @classmethod
    def find(cls, filter: dict[str, str | dict]) -> Any:
        
        cursor = cls._db.find(filter)
        return ModelCursor(cls, cursor) 

    @classmethod
    def aggregate(cls, pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor:
        return cls.db.aggregate(pipeline)
    
    @classmethod
    def find_by_id(cls, id: str) -> Self | None:
        """ 
        NO IMPLEMENTAR HASTA EL TERCER PROYECTO
        Busca un documento por su id utilizando la cache y lo devuelve.
        Si no se encuentra el documento, devuelve None.

        Parameters
        ----------
            id : str
                id del documento a buscar
        Returns
        -------
            Self | None
                Modelo del documento encontrado o None si no se encuentra
        """ 
        #TODO
        # buscar por clave (Un get básicamente y devolvemos el propio)
        return cls._redis.get(id)

    @classmethod
    def init_class(cls, redis_client:None, db_collection: pymongo.collection.Collection, indexes:dict[str,str], required_vars: set[str], admissible_vars: set[str]) -> None:
      

        cls._db = db_collection
        cls._redis = redis_client
        cls._required_vars = required_vars
        cls._admissible_vars = admissible_vars
        cls._location_var = indexes.get("location_index", None)

        
        if "unique_indexes" in indexes:
            for unique_index in indexes["unique_indexes"]:
                cls._db.create_index(unique_index, unique=True)
    
        if "regular_indexes" in indexes:
            for regular_index in indexes["regular_indexes"]:
                cls._db.create_index(regular_index, unique=False)

        if "location_index" in indexes:
            cls._db.create_index([(indexes["location_index"]+"_loc", pymongo.GEOSPHERE)], unique=False)
        



class ModelCursor:
    
    model_class: Model
    cursor: pymongo.cursor.Cursor

    def __init__(self, model_class: Model, cursor: pymongo.cursor.Cursor):

        self.model_class = model_class
        self.cursor = cursor
    
    def __iter__(self) -> Generator:
        
        while(self.cursor.alive == True):
            document = next(self.cursor)
            yield self.model_class(**document)

     
            


def initApp(definitions_path: str = "./models.yml", db_name=None, mongodb_uri=None, scope=globals()) -> None:
   
    #TODO 
    # Establecer configuración inicial de la Base de Datos REDIS
    # hacer la conexion y checkear y si tiene una cookie de sesión
    
    redis_cache = redis_conns["sesiones"] 
    try:
        redis_cache.config_set("maxmemory", "150mb")
        redis_cache.config_set("maxmemory-policy", "volatile-ttl")
        print("Config Redis aplicada (maxmemory + volatile-ttl)")
    except redis.exceptions.ResponseError as e:
        # Aquí estás en un Redis que no deja cambiar config en runtime
        print(" No se pudo aplicar config de Redis desde código:", e)
    
    
    try:
        redis_cache.ping()
        print("Te has conectado con exito a REDIS")
    except Exception as e: 
        print(e)

    
    client = MongoClient(mongodb_uri, server_api = ServerApi('1'))
    db = client[db_name]

    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    
    with open(definitions_path, 'r', encoding='utf-8') as file:
        models_definitions = yaml.safe_load(file)

    
    for class_name, class_def in models_definitions.items():
        
        new_cls = type(class_name, (Model,), {})
        
        scope[class_name] = new_cls

        
        db_collection = db[class_name]
        
        redis_client = redis_cache
        
        required_vars   = set(class_def.get("required_vars", []))
        admissible_vars = set(class_def.get("admissible_vars", []))

        
        
        admissible_vars |= required_vars

        
        admissible_vars.add("_id")

        
        loc_field = class_def.get("location_index", None)
        
        
        if loc_field:
            admissible_vars.add(f"{loc_field}_loc")

        
        indexes = {
            "unique_indexes": class_def.get("unique_indexes", []) or [],
            "regular_indexes": class_def.get("regular_indexes", []) or [],
            "location_index": loc_field
        }
        
        
        new_cls.init_class(
            db_collection=db_collection,
            redis_client = redis_client,
            indexes=indexes,
            required_vars=required_vars,
            admissible_vars=admissible_vars
        )

    Sesiones.initRedis(redis_cache)

def generate_token():
        #math.random
        token = random.randint(100000, 999999)
        return token


if __name__ == '__main__':
    
    # Inicializar base de datos y modelos con initApp
    #TODO
    initApp(mongodb_uri = MONGO_URI, db_name = DB_NAME, definitions_path = DEFINITIONS_PATH)
   

    ### ------------------ TEST CONEXION REDIS ------------------------------ ###

    

    # ya tenemos el 'cursor' de sesion en Redis, podemos testear ahora.

    # sacar de input nombre de usuario y contrasenia
    # init de sesion
    # en el init se crea el token y se guarda
    # si esta creado el token en redis se recupera sesion
    # si no existe se crea una nueva y se guarda en redis
    

    nombreUsuario = input("Introduce tu nombre de usuario: ")
    contrasenia = input("Introduce tu contrasenia: ")

    sesion = Sesiones(nombreUsuario, contrasenia, nombreUsuario, generate_token(), generate_token())
    
    print(f"Sesion creada para el usuario {nombreUsuario} con token {sesion._tokenSesion}")

    sesion_result = Sesiones.login(nombreUsuario, contrasenia)


    