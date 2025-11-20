import redis

class Sesiones:

    """
    
    Control de acceso al sistema mediante usuario y contrasenia 
    Los datos asociados al control de acceso serán almacenados en la base de datos
    Se almacenara cada usuario con su nombre completo, nombre de usuario,
    contrasenia, privilegios(un valor entero positivo aleatorio) y un token de sesion.
    Los datos de conexion ya los tenemos en el ODM
    
    """
    _redis = None
    _host = None
    
    def __init__(self, nombreUsuario, contrasenia, nombreCompleto, privilegios, tokenSesion):
        
        self._nombreUsuario = nombreUsuario
        self._contrasenia = contrasenia
        self._nombreCompleto = nombreCompleto
        self._privilegios = privilegios
        self._tokenSesion = tokenSesion

