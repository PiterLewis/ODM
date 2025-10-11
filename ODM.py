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


#diccionario global
CACHE: dict[str, Point | str] = {} #clave string valor Point
FAIL_MESSAGE = "Error: La direccion no se ha podido geolocalizar"

def getLocationPoint(address: str) -> Point | str: #en caso de error devuelve un str en cache
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
    
    if not address:
        return None
    
    #para no llamar a la Api siempre,
    #creamos un diccionario que actúa a modo de caché
    if address in CACHE:
        return CACHE[address] #devolvemos dato
    

    location = None
    attempts = 0
    while location is None:
        try:
            time.sleep(2)
            #TODO
            #prueba
            # Es necesario proporcionar un user_agent para utilizar la API
            # Utilizar un nombre aleatorio para el user_agent
            location = Nominatim(user_agent="santifer").geocode(address)
        except GeocoderTimedOut:
            # Puede lanzar una excepcion si se supera el tiempo de espera
            # Volver a intentarlo
            attempts +=1
            if attempts > 3:
                return None
            continue

    #TODO
    # Devolver un GeoJSON de tipo punto con la latitud y longitud almacenadas
    point = Point((location.longitude, location.latitude))
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
        self._data: dict[str, str | dict] = {}
        #TODO
        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion.
        for campo_requerido in self._required_vars:
              if campo_requerido not in kwargs:
            #lanza el error
                raise ValueError(f"El atributo requerido '{campo_requerido}' es obligatorio y no se ha proporcionado.")
              
        for atributo_perimitido in kwargs:
            if atributo_perimitido not in self._admissible_vars:
                raise ValueError(f"El atributo requerido '{atributo_perimitido}'no es admisible.")
        # Asigna todos los valores en kwargs a las atributos con 
        # nombre las claves en kwargs
        for atributosNuevos in list(kwargs.keys()):
            if atributosNuevos in self._addres_vars:
                direccion_valor = kwargs[atributosNuevos]
                punto = getLocationPoint(direccion_valor)
        # Utilizamos el atributo data para guardar los variables 
        # almacenadas en la base de datos en una solo atributo
                if punto: 
                    new_loc_key = f"{atributosNuevos}_loc"
                    kwargs[new_loc_key] = punto
                    self._admissible_vars.add(new_loc_key)
        # Encapsular los datos en una sola variable facilita la 
        # gestion en metodos como save.
        self._data.update(kwargs)

    def __setattr__(self, name: str, value: str | dict) -> None:
        """ Sobreescribe el metodo de asignacion de valores a los 
        atributos del objeto con el fin de controlar que atributos 
        son modificados y cuando son modificados.
        """
        if name in {'_modified_vars', '_required_vars', '_admissible_vars', '_db', '_location_var'}:
            super().__setattr__(name, value)
            return
        
        #TODO
        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion.

        
        # Asigna el valor value a la variable name
        self._data[name] = value

    def __getattr__(self, name: str) -> Any:
        """ Sobreescribe el metodo de acceso a atributos del objeto
        __getattr__ solo es llamado cuando no encuentra el atributo
        en el objeto 
        """
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

    def delete(self) -> None:
        """
        Elimina el modelo de la base de datos
        """
        #TODO
        if "_id" in self._data:
            self._db.delete_one({"_id": self._data["_id"]})
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
        cls._db = db_collection
        cls._required_vars = required_vars
        cls._admissible_vars = admissible_vars
        
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
            


def initApp(definitions_path: str = "./models.yml", mongodb_uri="mongodb://localhost:27017/", db_name="abd", scope=globals()) -> None:
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
    scope["MiModelo"] = type("MiModelo", (Model,),{})
    # Ignorar el warning de Pylance sobre MiModelo, es incapaz de detectar
    # que se ha declarado la clase en la linea anterior ya que se hace
    # en tiempo de ejecucion.

    with open(definitions_path, 'r', encoding='utf-8') as file:
        models_definitions = yaml.safe_load(file)

    
    for class_name, class_def in models_definitions.items():
        new_cls = type(class_name, (Model,), {})
        scope[class_name] = new_cls

        db_collection = db[class_name]

        required_vars = set(class_def.get("required_vars", []))
        admissible_vars = set(class_def.get("admissible_vars", []))

    
        #cargamos los indices
        indexes = { 
                    "unique_indexes": class_def.get("unique_indexes", []),
                    "regular_indexes": class_def.get("regular_indexes", []),
                    "location_index": class_def.get("location_index", None)
                    }
        
        new_cls.init_class(db_collection=db_collection, indexes=indexes, required_vars=required_vars, admissible_vars=admissible_vars)
    
    MiModelo.init_class(db_collection=None, indexes=None, required_vars=None, admissible_vars=None)

if __name__ == '__main__':
    
    # Inicializar base de datos y modelos con initApp
    #TODO
    initApp(mongodb_uri = "mongodb+srv://admin1234:Xhantiago2005@cluster0.hb27z86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
   

    

    #Inicializar los modelos  con initApp
    #Ejemplo
    m = MiModelo(nombre="Pablo", apellido="Ramos", edad=18)
    m.save()
    m.nombre="Pedro"
    print(m.nombre)

    # Hacer pruebas para comprobar que funciona correctamente el modelo
    #TODO
    # Crear modelo

    # Asignar nuevo valor a variable admitida del objeto 

    # Asignar nuevo valor a variable no admitida del objeto 

    # Guardar

    # Asignar nuevo valor a variable admitida del objeto

    # Guardar

    # Buscar nuevo documento con find

    # Obtener primer documento

    # Modificar valor de variable admitida

    # Guardar