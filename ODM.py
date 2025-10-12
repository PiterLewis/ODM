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
import os

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
DEFINITIONS_PATH = os.getenv("DEF_PATH")

#diccionario global
CACHE: dict[str, Point | str] = {} #clave string valor Point
FAIL_MESSAGE = "No se pudieron obtener coordenadas"
NOT_ADMITTED_VARIABLE = "No esta permitida usar esta variable"

def getLocationPoint(address: str) -> Point: #devuelve un objeto Point
    """ 
    Obtiene las coordenadas de una dirección en formato geojson.Point
    Utilizar la API de geopy para obtener las coordenadas de la direccion
    Cuidado, la API es publica tiene limite de peticiones, utilizar sleeps.

    Parameters
    ----------
        address : str
            direccion completa de la que obtener las coordenadas
    Returns
    -------
        geojson.Point
            coordenadas del punto de la direccion
    """
    
    #si no hay direccion lanza error
    if not address:
        raise ValueError(FAIL_MESSAGE)
    
    #para no llamar a la Api siempre,
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
    """ 
    Clase de modelo abstracta
    Crear tantas clases que hereden de esta clase como  
    colecciones/modelos se deseen tener en la base de datos.

    Attributes
    ----------
        required_vars : set[str]
            conjunto de atributos requeridos por el modelo
        admissible_vars : set[str]
            conjunto de atributos admitidos por el modelo
        db : pymongo.collection.Collection
            conexion a la coleccion de la base de datos
    
    Methods
    -------
        __setattr__(name: str, value: str | dict) -> None
            Sobreescribe el metodo de asignacion de valores a los 
            atributos del objeto con el fin de controlar qué atributos 
            son modificados y cuando son modificados.
        __getattr__(name: str) -> Any
            Sobreescribe el metodo de acceso a atributos del objeto 
        save()  -> None
            Guarda el modelo en la base de datos
        delete() -> None
            Elimina el modelo de la base de datos
        find(filter: dict[str, str | dict]) -> ModelCursor
            Realiza una consulta de lectura en la BBDD.
            Devuelve un cursor de modelos ModelCursor
        aggregate(pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor
            Devuelve el resultado de una consulta aggregate.
        find_by_id(id: str) -> dict | None
            Busca un documento por su id utilizando la cache y lo devuelve.
            Si no se encuentra el documento, devuelve None.
        init_class(db_collection: pymongo.collection.Collection, required_vars: set[str], admissible_vars: set[str]) -> None
            Inicializa las variables de clase en la inicializacion del sistema.

    """
    #variables de clase
    _required_vars: set[str]
    _admissible_vars: set[str]
    _location_var: None
    _db: pymongo.collection.Collection
    _internal_vars: set[str]={}
    

    def __init__(self, **kwargs: dict[str, str | dict]):
        """
        Inicializa el modelo con los valores proporcionados en kwargs
        Comprueba que los valores proporcionados en kwargs son admitidos
        por el modelo y que las atributos requeridos son proporcionadas.

        Parameters
        ----------
            kwargs : dict[str, str | dict]
                diccionario con los valores de las atributos del modelo
        """
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
        for k,v in kwargs.items():
            setattr(self, k, v) #usamos el __setattr__ sobreescrito para asignar los valores

        self._modified_vars.clear() #inicializamos el set de variables modificadas

    def __setattr__(self, name: str, value: str | dict) -> None:
        """ Sobreescribe el metodo de asignacion de valores a los 
        atributos del objeto con el fin de controlar que atributos 
        son modificados y cuando son modificados.
        """
        if name in {'_modified_vars', '_required_vars', '_admissible_vars', '_db', '_location_var', '_data'}:
            super().__setattr__(name, value)
            return
        
        #TODO
        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion.
        # Consulto la lista de reglas que me dio el arquitecto (initApp).
        if name not in self._admissible_vars:
            # Si no está en la lista, no está permitido. Deniego el acceso.
            raise AttributeError(f"El atributo '{name}' no es admitido por el modelo.")
        
        
        self._data[name] = value
        #marcamos variable como modificada
        self._modified_vars.add(name)

        #si es una direccion llama a la fuuncion getlocationPoint
        if name == self._location_var:
            location_point = getLocationPoint(value)
            if location_point is None:
                self._data[f"{self._location_var}_loc"] = FAIL_MESSAGE
            else:
                self._data[f"{self._location_var}_loc"] = location_point
                

    def __getattr__(self, name: str) -> Any:
        """ Sobreescribe el metodo de acceso a atributos del objeto
        __getattr__ solo es llamado cuando no encuentra el atributo
        en el objeto 
        """
        # ya estaba implementado
        if name in {'_modified_vars', '_required_vars', '_admissible_vars', '_db', '_data', '_location_var'}:
            return super().__getattribute__(name)
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError
        
    def save(self) -> None:
        """
        Guarda el modelo en la base de datos
        Si el modelo no existe en la base de datos, se crea un nuevo
        documento con los valores del modelo. En caso contrario, se
        actualiza el documento existente con los nuevos valores del
        modelo.
        """
        #TODO

        #si existe quiere decir que tiene un _id en la coleccion
        if "_id" in self._data:
        # Existe 
            update_doc = {
                k: self._data[k] # clave valor para cada variable en los datos del modelo
                for k in getattr(self, "_modified_vars", set()) #cogemos un empty set si no existe
                }
            #no actualizamos el _id, popeamos si existe
            update_doc.pop("_id", None)
            if update_doc:
                #query de actualizacion con los valores modificados
                self._db.update_one({"_id": self._data["_id"]}, {"$set": update_doc})
                #limpio las modificadas del object
                self._modified_vars.clear()
        else:
        # No existe aun
            result = self._db.insert_one(dict(self._data))
            self._data["_id"] = result.inserted_id
            self._modified_vars.clear()

    def delete(self) -> None:
        """
        Elimina el modelo de la base de datos
        """
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
        """ 
        Utiliza el metodo find de pymongo para realizar una consulta
        de lectura en la BBDD.
        find debe devolver un cursor de modelos ModelCursor

        Parameters
        ----------
            filter : dict[str, str | dict]
                diccionario con el criterio de busqueda de la consulta
        Returns
        -------
            ModelCursor
                cursor de modelos
        """ 
        #TODO
        # cls es el puntero a la clase
        # utilizamos el atributo de clase _db para hacer la consulta
        # y devolvemos un ModelCursor
        cursor = cls._db.find(filter)
        return ModelCursor(cls, cursor) #cls es el model class y cursor es el cursor iterador

    @classmethod
    def aggregate(cls, pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor:
        """ 
        Devuelve el resultado de una consulta aggregate. 
        No hay nada que hacer en esta funcion.
        Se utilizara para las consultas solicitadas
        en el segundo proyecto de la practica.

        Parameters
        ----------
            pipeline : list[dict]
                lista de etapas de la consulta aggregate 
        Returns
        -------
            pymongo.command_cursor.CommandCursor
                cursor de pymongo con el resultado de la consulta
        """ 
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
        pass

    @classmethod
    def init_class(cls, db_collection: pymongo.collection.Collection, indexes:dict[str,str], required_vars: set[str], admissible_vars: set[str]) -> None:
        """ 
        Inicializa los atributos de clase en la inicializacion del sistema.
        Aqui se deben inicializar o asegurar los indices. Tambien se puede
        alguna otra inicialización/comprobaciones o cambios adicionales
        que estime el alumno.

        Parameters
        ----------
            db_collection : pymongo.collection.Collection
                Conexion a la collecion de la base de datos.
            indexes: Dict[str,str]
                Set de indices y tipo de indices para la coleccion
            required_vars : set[str]
                Set de atributos requeridos por el modelo
            admissible_vars : set[str] 
                Set de atributos admitidos por el modelo
        """
        #asignacion de variables de clase
        cls._db = db_collection
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
    """ 
    Cursor para iterar sobre los documentos del resultado de una
    consulta. Los documentos deben ser devueltos en forma de objetos
    modelo.

    Attributes
    ----------
        model_class : Model
            Clase para crear los modelos de los documentos que se iteran.
        cursor : pymongo.cursor.Cursor
            Cursor de pymongo a iterar

    Methods
    -------
        __iter__() -> Generator
            Devuelve un iterador que recorre los elementos del cursor
            y devuelve los documentos en forma de objetos modelo.
    """

    #asignacion de variables de clase
    model_class: Model
    cursor: pymongo.cursor.Cursor

    def __init__(self, model_class: Model, cursor: pymongo.cursor.Cursor):
        """
        Inicializa el cursor con la clase de modelo y el cursor de pymongo

        Parameters
        ----------
            model_class : Model
                Clase para crear los modelos de los documentos que se iteran.
            cursor: pymongo.cursor.Cursor
                Cursor de pymongo a iterar
        """
        # inicializacion de variables de clase
        self.model_class = model_class
        self.cursor = cursor
    
    def __iter__(self) -> Generator:
        """
        Devuelve un iterador que recorre los elementos del cursor
        y devuelve los documentos en forma de objetos modelo.
        Utilizar yield para generar el iterador
        Utilizar la funcion next para obtener el siguiente documento del cursor
        Utilizar alive para comprobar si existen mas documentos.
        """
        #TODO
        while(self.cursor.alive == True):
        #no nos da el ptr al primer elemento sino que es un iterador,
        #por lo que hay que avanzarlo antes de aniadir.
            document = next(self.cursor)
            yield self.model_class(**document) #llamada al constructor y le pasamos dict de valores

        #for doc in self.cursor:                  
        #yield self.model_class(**doc)        
            


def initApp(definitions_path: str = "./models.yml", mongodb_uri= "mongodb+srv://admin1234:Xhantiago2005@cluster0.hb27z86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" ,db_name="practica_mongo", scope=globals()) -> None:
    """ 
    Declara las clases que heredan de Model para cada uno de los 
    modelos de las colecciones definidas en definitions_path.
    Inicializa las clases de los modelos proporcionando los indices y 
    atributos admitidos y requeridos para cada una de ellas y la conexión a la
    collecion de la base de datos.
    
    Parameters
    ----------
        definitions_path : str
            ruta al fichero de definiciones de modelos
        mongodb_uri : str
            uri de conexion a la base de datos
        db_name : str
            nombre de la base de datos
    """
    #TODO
    # Inicializar base de datos
    client = MongoClient(mongodb_uri, server_api = ServerApi('1'))
    db = client[db_name]
    # Create a new client and connect to the server
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    #TODO
    # Declarar tantas clases modelo colecciones existan en la base de datos
    # Leer el fichero de definiciones de modelos para obtener las colecciones,
    # indices y los atributos admitidos y requeridos para cada una de ellas.
    # Ejemplo de declaracion de modelo para colecion llamada MiModelo
    #scope["MiModelo"] = type("MiModelo", (Model,),{})
    # Ignorar el warning de Pylance sobre MiModelo, es incapaz de detectar
    # que se ha declarado la clase en la linea anterior ya que se hace
    # en tiempo de ejecucion.

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
            indexes=indexes,
            required_vars=required_vars,
            admissible_vars=admissible_vars
        )

    #debugging
    #print(new_cls)
    #print(new_cls._db)
    #print(new_cls._required_vars)
    #print(new_cls._admissible_vars)
    #MiModelo.init_class(db_collection=None, indexes=None, required_vars=None, admissible_vars=None)

if __name__ == '__main__':
    
    # Inicializar base de datos y modelos con initApp
    #TODO
    initApp(mongodb_uri = MONGO_URI, db_name = DB_NAME, definitions_path = DEFINITIONS_PATH)
   

    #Inicializar los modelos  con initApp
    #Ejemplo
    #m = MiModelo(nombre="Pablo", apellido="Ramos", edad=18)
    #m.save()
    #m.nombre="Pedro"
    #print(m.nombre)

    # Hacer pruebas para comprobar que funciona correctamente el modelo
    #TODO
    # Crear modelo
    print("Creando persona")
    p = persona(
        nombre="Santiago Garcia Dominguez",
        dni="134366666J",
        mail="miemail@outlok.es",
        telefono="+1 222 333 4444",
        contactos_emergencia=["+34 666 111 222", "+34 666 333 444"],
        direccion="Calle Bergantin 39, Playa Honda",   
    ) #no me doxeen que es real 
    print("Persona creada")
    # Asignar nuevo valor a variable admitida del objeto 
    p.nombre = "Santiago G. Dominguez"
    p.direccion = "Calle Real 1, Madrid"
    # Asignar nuevo valor a variable no admitida del objeto 
    try:
        p.edad = 20
    except AttributeError as e:
        print(NOT_ADMITTED_VARIABLE + " " + "STACK TRACE :", e)
    # Guardar
    p.save()
    print("Insert en persona hecho, _id =", p._data.get("_id"))
    # Asignar nuevo valor a variable admitida del objeto
    p.descripcion = "Practicas remuneradas"
    # Guardar
    p.save()
    print("Update persona OK")
    # Buscar nuevo documento con find
    current = persona.find({"nombre": "Santiago G. Dominguez"})
    # Obtener primer documento
    p2 = next(iter(current), None)
    if p2 is None:
        raise RuntimeError("No se encontró la persona, pruebe con otro valor")
    # Modificar valor de variable admitida
    p.contactos_emergencia = ["+34 600 999 999", "+34 600 888 888"]
    # Guardar
    p.save()
    print("Update persona OK")

    print("NAMESPACE:", persona._db.full_name)  # debería ser 'practica_mongo.persona'
    print("DB usada:", persona._db.database.name)
    print("Colección:", persona._db.name)