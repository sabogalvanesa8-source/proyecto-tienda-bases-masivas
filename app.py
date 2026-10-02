# Importamos la aplicación desde servidor.py
from servidor import app

# Ejecutamos la aplicación directamente
if __name__ == "__main__":

    # Iniciamos el servidor en el puerto 5000
    app.run(
        # Permite conexiones externas
        host="0.0.0.0",

        # Puerto donde funciona la aplicación
        port=5000,

        # Desactiva el modo de depuración
        debug=False
    )