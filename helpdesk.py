import redis

class HelpDesk:
    _redis = None

    @classmethod
    def initRedis(cls, redis_client):
        cls._redis = redis_client

    @classmethod
    def solicitar_ayuda(cls, usuario_id, prioridad):
        """
        Registra una petición de ayuda de un usuario con una prioridad.
        
        Args:
            usuario_id (str): Identificador del usuario.
            prioridad (int): Prioridad de la petición (mayor valor = mayor prioridad).
        """
        if cls._redis:
            # Usamos un Sorted Set
            # Clave: "helpdesk_queue"
            # Valor: usuario_id
            # Score: prioridad
            cls._redis.zadd("helpdesk_queue", {str(usuario_id): prioridad})

    @classmethod
    def atender_usuario(cls):
        """
        Obtiene la petición de mayor prioridad y la elimina de la cola.
        Si no hay peticiones, se bloquea hasta que llegue una.
        
        Returns:
            str: El usuario_id de la petición atendida.
        """
        if cls._redis:
            # bzpopmax elimina y devuelve el miembro con mayor score de un sorted set.
            # Bloquea si está vacío. timeout=0 indica bloqueo indefinido.
            # Retorna una tupla: (key, member, score)
            resultado = cls._redis.bzpopmax("helpdesk_queue", timeout=0)
            
            if resultado:
                # resultado[0] es la key ("helpdesk_queue")
                # resultado[1] es el miembro (usuario_id)
                # resultado[2] es el score (prioridad)
                return resultado[1]
        return None
