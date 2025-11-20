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

    #si no hay direccion lanza error
    if not address:
        raise ValueError(FAIL_MESSAGE)
    
    #creamos un diccionario que actúa a modo de caché
    if address in CACHE:
        return CACHE[address] #devolvemos dato
    

    #valores iniciales
    location = None
    attempts = 0

    #intenta obtener la localizacion
    while location is None:
        try:
            time.sleep(2)
            #TODO
            # Es necesario proporcionar un user_agent para utilizar la API
            # Utilizar un nombre aleatorio para el user_agent
            location = Nominatim(user_agent="santifer").geocode(address)
        except GeocoderTimedOut:
            attempts +=1
            #excepcion si supera los attempts
            if attempts >= 3:
                raise ValueError(FAIL_MESSAGE)
            continue

    #TODO
    # Devolver un GeoJSON de tipo punto con la latitud y longitud almacenadas
    point = Point((location.longitude, location.latitude))
    #asignacion a la cache
    CACHE[address] = point
    return point


class Model:
    
    #variables de clase
    _required_vars: set[str]
    _admissible_vars: set[str]
    _location_var: None
    _db: pymongo.collection.Collection
    _internal_vars: set[str]={}
    _redis = None

    def __init__(self, **kwargs: dict[str, str | dict]):
        #diccionario de datos del objeto
        self._data: dict[str, str | dict] = {}
        
        #TODO
        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion.
        super().__setattr__("_data", {}) #inicializamos el diccionario de datos
        super().__setattr__("_modified_vars", set()) #inicializamos el set de variables modificadas


        # por cada campo requerido comprueba que esta en los argumentos que le paasamos 
        for campo_requerido in self._required_vars:
              if campo_requerido not in kwargs:
                #lanza excepcion si no hay campo requerido
                raise ValueError(f"El atributo requerido '{campo_requerido}' es obligatorio y no se ha proporcionado.")
        
        #por cada permitido comprueba que esta en los argumentos que le pasamos
        for atributo_perimitido in kwargs:
            if atributo_perimitido not in self._admissible_vars:
                raise ValueError(f"El atributo requerido '{atributo_perimitido}'no es admisible.")
        
        #asignacion de valores a los atributos del objeto
        self._data.update(kwargs)


    def __setattr__(self, name: str, value: str | dict) -> None:

        if name in {'_modified_vars', '_required_vars', '_admissible_vars', '_db', '_location_var', '_data'}:
            super().__setattr__(name, value)
            return

        #TODO
        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion.
          #    Consulto la lista de reglas que me dio el arquitecto (initApp).
        if name not in self._admissible_vars:
            # Si no está en la lista, no está permitido. Deniego el acceso.
            raise AttributeError(f"El atributo '{name}' no es admitido por el modelo.")


        self._data[name] = value
        #    para usar el metodo save
        self._modified_vars.add(name)

        #si es una direccion llama a la fuuncion getlocationPoint
        if name == self._location_var:
            location_point = getLocationPoint(value)
            if location_point is None:
                self._data[f"{self._location_var}_loc"] = FAIL_MESSAGE
            else:
                self._data[f"{self._location_var}_loc"] = location_point


    def __getattr__(self, name: str) -> Any:
        # ya estaba implementado
        if name in {'_modified_vars', '_required_vars', '_admissible_vars', '_db', '_data', '_location_var'}:
            return super().__getattribute__(name)
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError
        
    def save(self) -> None:

        #TODO
        if "_id" in self._data:
        # Existe
            update_doc = {k: self._data[k] for k in getattr(self, "_modified_vars", set())}
            update_doc.pop("_id", None)
            if update_doc:
                self._db.update_one({"_id": self._data["_id"]}, {"$set": update_doc})
                self._modified_vars.clear()
        else:
        # No existe aun
            result = self._db.insert_one(dict(self._data))
            self._data["_id"] = result.inserted_id
            self._modified_vars.clear()


        ## ACABAMOS DE CREAR UN OBJETO E INSERTADO EN LA BBDD MONGO
        ## AHORA HAY QUE INSERTARLO EN REDIS PARA TENERLO CACHEADO.
        if self._redis and "nombre" in self._data:
            key = f"{self.__class__.__name__}:{self._data['nombre']}"
            self._redis.setex(key, 86400, json.dumps(self._data))

    def delete(self) -> None:
        #TODO
        #si el modelo existe(tiene id) lo elimina
        if "_id" in self._data:
            self._db.delete_one({"_id": self._data["_id"]})
            #como aditivo limpio los datos del objeto
            self._data.clear()
            self._modified_vars.clear()
        else:
            raise ValueError("El modelo no existe en la base de datos.")
    
    @classmethod
    def find(cls, filter: dict[str, str | dict]) -> Any:
        #TODO
        # cls es el puntero a la clase
        # utilizamos el atributo de clase _db para hacer la consulta
        # y devolvemos un ModelCursor
        cursor = cls._db.find(filter)
        return ModelCursor(cls, cursor) #cls es el model class y cursor es el cursor iterador

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
      
        #asignacion de variables de clase
        cls._db = db_collection
        cls._redis = redis_client
        cls._required_vars = required_vars
        cls._admissible_vars = admissible_vars
        cls._location_var = indexes.get("location_index", None)

        
        # TODO
        # aniadir unique_indexes, regular_indexes y location_index
        if "unique_indexes" in indexes:
            for unique_index in indexes["unique_indexes"]:
                cls._db.create_index(unique_index, unique=True)
    
        if "regular_indexes" in indexes:
            for regular_index in indexes["regular_indexes"]:
                cls._db.create_index(regular_index, unique=False)

        if "location_index" in indexes:
            cls._db.create_index([(indexes["location_index"]+"_loc", pymongo.GEOSPHERE)], unique=False)
        



class ModelCursor:
    #asignacion de variables de clase
    model_class: Model
    cursor: pymongo.cursor.Cursor

    def __init__(self, model_class: Model, cursor: pymongo.cursor.Cursor):

        # inicializacion de variables de clase
        self.model_class = model_class
        self.cursor = cursor
    
    def __iter__(self) -> Generator:
        #TODO
        while(self.cursor.alive == True):
            document = next(self.cursor)
            yield self.model_class(**document) #llamada al constructor y le pasamos dict de valores

     
            


def initApp(definitions_path: str = "./models.yml", db_name=None, mongodb_uri=None, scope=globals()) -> None:
   
    #TODO 
    # Establecer configuración inicial de la Base de Datos REDIS
    # hacer la conexion y checkear y si tiene una cookie de sesión

    redis_cache = redis_conns["cache"] # me devuelve la conexion de redis
    
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

    #TODO
    # Inicializar base de datos
    client = MongoClient(mongodb_uri, server_api = ServerApi('1'))
    db = client[db_name]

    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    #leemos el yml
    with open(definitions_path, 'r', encoding='utf-8') as file:
        models_definitions = yaml.safe_load(file)

    
    #por cada modelo definido en el yml
    for class_name, class_def in models_definitions.items():
        #creamos la clase dinamicamente
        new_cls = type(class_name, (Model,), {})
        #la asignamos al scope
        scope[class_name] = new_cls

        #obtenemos la coleccion de la base de datos
        db_collection = db[class_name]
        
        redis_client = redis_cache
        #obtenemos los atributos requeridos y admitiodos
        required_vars   = set(class_def.get("required_vars", []))
        admissible_vars = set(class_def.get("admissible_vars", []))

        
        #los atributos requeridos son siempre admitidos
        admissible_vars |= required_vars

        #el _id siempre es un atributo admitido
        admissible_vars.add("_id")

        #si hay location_index aniadimos el atributo _loc
        loc_field = class_def.get("location_index", None)
        
        # si hay campo de localizacion aniadimos el campo _loc 
        # en los valores de location_index y agregamos a admitidos
        if loc_field:
            admissible_vars.add(f"{loc_field}_loc")

        #preparamos los indices
        indexes = {
            "unique_indexes": class_def.get("unique_indexes", []) or [],
            "regular_indexes": class_def.get("regular_indexes", []) or [],
            "location_index": loc_field
        }
        
        #inicializamos la clase
        new_cls.init_class(
            db_collection=db_collection,
            redis_client = redis_client,
            indexes=indexes,
            required_vars=required_vars,
            admissible_vars=admissible_vars
        )

if __name__ == '__main__':
    
    # Inicializar base de datos y modelos con initApp
    #TODO
    initApp(mongodb_uri = MONGO_URI, db_name = DB_NAME, definitions_path = DEFINITIONS_PATH)
   

    ### ------------------ TEST CONEXION REDIS ------------------------------ ###

    

    # ya tenemos el 'cursor' de sesion en Redis, podemos testear ahora.

    # Sample de ejecución del GET
    p = persona.find_by_id("ferniBerni")
    print(p)

    p = persona(nombre = "Santiago", dni = "6", mail= "xhantiago2005@gmail.com", universidad = "pu-tad")
    p.save()

    p = persona.find_by_id("Santiago")
    print(p)

    #Inicializar los modelos  con initApp
    #Ejemplo
    #m = MiModelo(nombre="Pablo", apellido="Ramos", edad=18)
    #m.save()
    #m.nombre="Pedro"
    #print(m.nombre)

    # Hacer pruebas para comprobar que funciona correctamente