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
from helpdesk import HelpDesk

load_dotenv()

REDIS_PASSWORD = os.getenv("REDIS_PSSWD")
REDIS_UNAME = os.getenv("REDIS_UNAME")
REDIS_HOST = os.getenv("REDIS_HOST")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
DEFINITIONS_PATH = os.getenv("DEF_PATH")


redis_client = redis.Redis(host=REDIS_HOST,port=11207,db=0,username=REDIS_UNAME,password=REDIS_PASSWORD,decode_responses=True)
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
        
            # Actualización
            update_doc = {k: self._data[k] for k in getattr(self, "_modified_vars", set())}
            update_doc.pop("_id", None)

            if update_doc:
                self._db.update_one({"_id": self._data["_id"]}, {"$set": update_doc})
                
                #Actualizar cache
                if self._redis:
                    cache_key = f"cache:{self.__class__.__name__}:{str(self._data['_id'])}"
                    doc_serializable = dict(self._data)
                    doc_serializable["_id"] = str(self._data["_id"])
                    self._redis.setex(cache_key, 86400, json.dumps(doc_serializable))
                
                self._modified_vars.clear()
            
        else:
            
            # Inserción nueva
            result = self._db.insert_one(dict(self._data))
            self._data["_id"] = result.inserted_id
            
            #Guardar en cache
            if self._redis:
                cache_key = f"cache:{self.__class__.__name__}:{str(result.inserted_id)}"
                doc_serializable = dict(self._data)
                doc_serializable["_id"] = str(self._data["_id"])
                self._redis.setex(cache_key, 86400, json.dumps(doc_serializable))
            
            self._modified_vars.clear()


    def delete(self) -> None:
        
        if "_id" in self._data:
            # Eliminar de cache primero
            if self._redis:
                cache_key = f"cache:{self.__class__.__name__}:{str(self._data['_id'])}"
                self._redis.delete(cache_key)
        
            # Eliminar de MongoDB
            self._db.delete_one({"_id": self._data["_id"]})
            self._data.clear()
            self._modified_vars.clear()

        else:
            raise ValueError("El modelo no existe en la base de datos.")
    
    @classmethod
    def delete_all(cls) -> None:
        """
        SOLO POR COMODIDAD Y NO CAMBIAR EL DNI DEL USUARIO DE PRUEBA EN CADA TEST
        """
        return cls._db.delete_many({})
    
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

        # Construir la clave para buscar en Redis
        cache_key = f"cache:{cls.__name__}:{id}"

        # Intentar obtener del USUARIO en Redis
        cached_data = cls._redis.get(cache_key)
        
        if cached_data:
            #CACHE HIT
            # Renovar el TTL
            cls._redis.expire(cache_key, 86400)
            
            # Convertir el JSON de Redis a diccionario Python
            doc_dict = json.loads(cached_data)
            
            # Convertir _id de string a ObjectId
            if "_id" in doc_dict:
                doc_dict["_id"] = ObjectId(doc_dict["_id"])
            
            # Devolver la información del usuario en cache
            return cls(**doc_dict)
        
        #CACHE MISS
        # Ahora buscamos en MongoDB
        doc = cls._db.find_one({"_id": ObjectId(id)})
        
        if not doc:
            # No existe ni en cache ni en MongoDB
            print("No existe el documento con id: ", id)
            return None
        
        # Usuario encontrado en MongoDB
        # Preparar los datos para guardar en Redis
        doc_serializable = dict(doc)
        doc_serializable["_id"] = str(doc["_id"])
        
        # Cachear en Redis, cache key -> la clave que hemos construido, 86k -> ttl, json.dumps -> convertir a string
        cls._redis.setex(cache_key, 86400,json.dumps(doc_serializable))
        
        # Devolver la información del usuario esta vez de mongo
        return cls(**doc)

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
    
    redis_cache = redis_client
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

    
    Sesiones.initRedis(redis_client)
    HelpDesk.initRedis(redis_client)

def generate_token():
        #math.random
        token = random.randint(100000, 999999)
        return token


if __name__ == '__main__':
    
    # Inicializar base de datos y modelos con initApp
    #TODO
    initApp(mongodb_uri = MONGO_URI, db_name = DB_NAME, definitions_path = DEFINITIONS_PATH)
   

    persona.delete_all()
    redis_client.flushdb()
    
    # Test de cache
    # crear y guardar persona
    p1 = persona(
        nombre="Lucia Alonso",
        dni="00000001C",
        mail="lucia.alonso@example.com",
        universidad=[
            {
                "codigo": "UPM01",
                "nombre": "UPM",
                "titulo": "Grado en Ingenieria Informatica",
                "fecha_fin": "2018-06-30"
            }
        ],
        telefono="+34 611 111 111",
        contactos_emergencia=["+34 691 111 111"],
        direccion="C/ Gran Via 12, Madrid, 28013",
        empresa=[
            {
                "nombre": "Google",
                "cargo": "Data Scientist",
                "fecha_inicio": "2021-02-01",
                "fecha_fin": None,
                "ciudad": "Madrid"
            }
        ],
        descripcion="Cientifica de datos en Google"
    )
    p1.save()
    print(f"persona guardada con id: {p1._id}")
    
    # verificar que se guardo en cache
    cache_key = f"cache:persona:{p1._id}"
    print(f"existe en cache: {bool(redis_client.exists(cache_key))}") #true o false
    
    # buscar por id viene de cache
    id_persona = str(p1._id)
    p2 = persona.find_by_id(id_persona)
    print(f"primera busqueda de cache: {p2.nombre}")
    
    # modificar y actualizar
    p2.telefono = "+34 622 222 222"
    p2.save()
    print(f"persona modificada, cache actualizado")
    
    # verificar modificacion
    p3 = persona.find_by_id(id_persona)
    print(f"telefono actualizado: {p3.telefono}\n")

    # Test de sesiones
    # registrar usuario
    usuario = "fernando"
    password = "1234"
    nueva_sesion = Sesiones(usuario, password, "Fernando Contreras")
    
    if nueva_sesion.registrar():
        print(f"usuario {usuario} registrado")
    else:
        print(f"usuario {usuario} ya existia")
    
    # login con usuario y password
    privilegios, token = Sesiones.login(usuario, password)
    if privilegios != -1:
        print(f"login exitoso - privilegios: {privilegios}, token: {token}")
    else:
        print("login fallido")
    
    # login con token
    if token:
        priv = Sesiones.login_token(token)
        if priv != -1:
            print(f"sesion recuperada con token - privilegios: {priv}")
        else:
            print("token invalido")
    
    # mostrar claves de sesiones
    sesion_keys = redis_client.keys('sesiones:*')
    print(f"claves de sesiones en redis: {len(sesion_keys)}\n")
    
    #Test helpdesk
    # registrar peticiones
    HelpDesk.solicitar_ayuda("user123", 5)
    HelpDesk.solicitar_ayuda("user456", 10)
    HelpDesk.solicitar_ayuda("user789", 3)
    print("3 peticiones registradas con prioridades: 5, 10, 3")
    
    # atender por orden de prioridad
    print(f"atendiendo 1: {HelpDesk.atender_usuario()} ")
    print(f"atendiendo 2: {HelpDesk.atender_usuario()} ")
    print(f"atendiendo 3: {HelpDesk.atender_usuario()} ")
    
    # verificar cola vacia
    pendientes = redis_client.zcard("sesiones:helpdesk_queue")
    print(f"peticiones pendientes: {pendientes}\n")
    
    #Test de cache miss por si falla la expiracion
    #crear segunda persona
    p5 = persona(
        nombre="Carlos Martinez",
        dni="00000002D",
        mail="carlos.martinez@example.com",
        universidad=[],
        telefono="+34 633 333 333",
        contactos_emergencia=["+34 699 999 999"],
        direccion="C/ Alcala 50, Madrid, 28014",
        empresa=[],
        descripcion="Desarrollador Backend"
    )
    
    p5.save()
    print(f"segunda persona guardada: {p5._id}")
    
    # eliminar de cache para simular expiracion
    cache_key_p5 = f"cache:persona:{p5._id}"
    redis_client.delete(cache_key_p5)
    print("cache eliminado manualmente")
    
    # buscar ir a mongo y recachear
    id_p5 = str(p5._id)
    p6 = persona.find_by_id(id_p5)
    print(f"encontrado en mongodb: {p6.nombre}")
    print(f"esta ahora en cache: {bool(redis_client.exists(cache_key_p5))}\n")
    
    #Test de delete    
    # verificar que esta en cache
    print(f"en cache antes de delete: {bool(redis_client.exists(cache_key_p5))}")
    
    # eliminar
    p6.delete()
    print("persona eliminada de mongodb")
    
    # verificar que se elimino de cache
    print(f"en cache despues de delete: {bool(redis_client.exists(cache_key_p5))}")
    
    # intentar buscar
    p7 = persona.find_by_id(id_p5)
    if p7 is None:
        print("busqueda devuelve None correctamente\n")
    else:
        print("error: persona todavia existe\n")
    
    #Test resumen final
    print(f"total de claves en redis: {redis_client.dbsize()}")
    
    # cache
    cache_keys = redis_client.keys('cache:*')
    print(f"\nclaves de cache: {len(cache_keys)}")
    for key in cache_keys:
        ttl = redis_client.ttl(key)
        print(f"  {key} (ttl: {ttl}s)")
    
    # sesiones
    sesion_keys = redis_client.keys('sesiones:*')
    print(f"\nclaves de sesiones: {len(sesion_keys)}")
    for key in sesion_keys:
        tipo = redis_client.type(key)
        if tipo == 'string':
            ttl = redis_client.ttl(key)
            print(f"  {key} (ttl: {ttl}s)")
        elif tipo == 'hash':
            print(f"  {key} (hash)")
        elif tipo == 'zset':
            size = redis_client.zcard(key)
            print(f"  {key} (zset, {size} items)")
    
    print("\ntests completados")
    

    