from flask import Flask, render_template, jsonify
import json
import os


# Creamos la aplicación Flask
app = Flask(__name__)


# Archivo donde está guardado el estado
ESTADO_FILE = "estado.json"


# Ruta principal de la página
@app.route("/")
def inicio():

    # Mostramos la interfaz web
    return render_template("index.html")


# Ruta para consultar el estado de los productos
@app.route("/api/estado")
def obtener_estado():

    # Verificamos si existe el archivo
    if not os.path.exists(ESTADO_FILE):

        return jsonify({
            "error": "estado.json no existe"
        }), 404


    try:

        # Abrimos y leemos el estado
        with open(
            ESTADO_FILE,
            "r",
            encoding="utf-8"
        ) as archivo:

            estado = json.load(archivo)


        # Enviamos los datos en formato JSON
        return jsonify(estado)


    except Exception as error:

        # Mostramos el error si ocurre algún problema
        return jsonify({
            "error": str(error)
        }), 500


# Iniciamos el servidor directamente
if __name__ == "__main__":

    app.run(
        # Permite conexiones externas
        host="0.0.0.0",

        # Puerto de la aplicación
        port=5000,

        # Desactivamos el modo debug
        debug=False
    )