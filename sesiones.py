import redis
import random
import time

class Sesiones:
    _redis = None

    def __init__(self, nombreUsuario, contrasenia, nombreCompleto=None, privilegios=None):
        self.nombreUsuario = nombreUsuario
        self.contrasenia = contrasenia
        self.nombreCompleto = nombreCompleto

        # Si no se pasan privilegios, generar uno aleatorio
        self.privilegios = privilegios if privilegios else random.randint(1, 100)
        self.tokenSesion = None

    #Crear usuario (Registrar)
    def registrar(self):
        """Guarda los datos del usuario en Redis (sin token aún)"""
        clave_usuario = f"user:{self.nombreUsuario}"
        
        if self._redis.exists(clave_usuario):
            print("El usuario ya existe.")
            return False

        datos = {
            "nombreCompleto": self.nombreCompleto,
            "contrasenia": self.contrasenia,
            "privilegios": self.privilegios,
            "tokenSesion": "" # Inicialmente vacío
        }
        self._redis.hset(clave_usuario, mapping=datos)
        return True

    #Login con Usuario/Pass (Genera nueva sesión)
    @classmethod
    def login(cls, nombreUsuario, contrasenia):
        clave_usuario = f"user:{nombreUsuario}"

        if not cls._redis.exists(clave_usuario):
            return -1

        # Verificar contraseña
        pass_guardada = cls._redis.hget(clave_usuario, "contrasenia")
        if pass_guardada != contrasenia:
            return -1

        # Generar nuevo token
        nuevo_token = str(random.randint(100000, 999999))
        privilegios = cls._redis.hget(clave_usuario, "privilegios")

        # Actualizar el token en el perfil del usuario
        cls._redis.hset(clave_usuario, "tokenSesion", nuevo_token)

        # Crear la clave de sesión con expiración
        clave_sesion = f"session:{nuevo_token}"
        cls._redis.set(clave_sesion, nombreUsuario, ex=20000) 

        return int(privilegios), nuevo_token

    # Login con Token
    @classmethod
    def login_token(cls, token):
        clave_sesion = f"session:{token}"

        # Si la clave no existe, la sesión expiró
        if not cls._redis.exists(clave_sesion):
            return -1

        # Recuperamos el usuario dueño de la sesión
        nombreUsuario = cls._redis.get(clave_sesion)
        clave_usuario = f"user:{nombreUsuario}"

        # Devolver privilegios
        privilegios = cls._redis.hget(clave_usuario, "privilegios")
        return int(privilegios)

    @classmethod
    def initRedis(cls, redis_client):
        cls._redis = redis_client