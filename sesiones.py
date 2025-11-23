import redis
import random
import time

class Sesiones:
    _redis = None

    def __init__(self, nombreUsuario, contrasenia, nombreCompleto=None, privilegios=None):
        self.nombreUsuario = nombreUsuario
        self.contrasenia = contrasenia
        self.nombreCompleto = nombreCompleto

        #Si no se pasan privilegios, le asigno uno aleatorio entre 1 y 100
        self.privilegios = privilegios if privilegios else random.randint(1, 100)
        self.tokenSesion = None

    #creamos usuario con registrar
    def registrar(self):
        """Guarda los datos del usuario en Redis (sin token aún)"""
        clave_usuario = f"sesiones:user:{self.nombreUsuario}"
        
        if self._redis.exists(clave_usuario):
            print("El usuario ya existe.")
            return False

        datos = {
            "nombreCompleto": self.nombreCompleto,
            "contrasenia": self.contrasenia,
            "privilegios": self.privilegios,
            "tokenSesion": "" #sin token aun
        }

        self._redis.hset(clave_usuario, mapping=datos)
        return True

    # login con user y pass, nos genera un token de sesion
    @classmethod
    def login(cls, nombreUsuario, contrasenia):
        clave_usuario = f"sesiones:user:{nombreUsuario}"

        if not cls._redis.exists(clave_usuario):
            return -1

        #verificar contraseña
        pass_guardada = cls._redis.hget(clave_usuario, "contrasenia")
        if pass_guardada != contrasenia:
            return -1

        #generar nuevo token
        nuevo_token = str(random.randint(100000, 999999))
        privilegios = cls._redis.hget(clave_usuario, "privilegios")

        #actualizar el token en el perfil del usuario
        cls._redis.hset(clave_usuario, "tokenSesion", nuevo_token)

        #crear la clave de sesión con expiración
        clave_sesion = f"sesiones:session:{nuevo_token}"
        cls._redis.set(clave_sesion, nombreUsuario, ex=30*24*60*60) # entiendo que el mes tiene 30 dias, si no solo tendria que tocar el m number 30

        return int(privilegios), nuevo_token

    #login con Token de sesion
    @classmethod
    def login_token(cls, token):
        clave_sesion = f"sesiones:session:{token}"

        # Si la clave no existe, la sesión expiró
        if not cls._redis.exists(clave_sesion):
            return -1

        #recuperamos el usuario dueño de la sesión
        nombreUsuario = cls._redis.get(clave_sesion)
        clave_usuario = f"sesiones:user:{nombreUsuario}"

        #devolvemos privilegios
        privilegios = cls._redis.hget(clave_usuario, "privilegios")
        return int(privilegios)

    @classmethod
    def initRedis(cls, redis_client):
        cls._redis = redis_client