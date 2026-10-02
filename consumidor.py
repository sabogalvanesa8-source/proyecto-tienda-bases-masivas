import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


# Definimos dónde están los eventos y el archivo de estado
CARPETA_EVENTOS = "data/eventos"
ARCHIVO_ESTADO = "estado.json"


# Definimos el inventario inicial
INVENTARIO_INICIAL = {
    "Arroz": 100,
    "Leche": 80,
    "Aceite": 60
}


# Definimos los precios iniciales
PRECIOS_INICIALES = {
    "Arroz": 10000,
    "Leche": 8000,
    "Aceite": 12000
}


# Copiamos los datos iniciales para trabajar con ellos
inventario = INVENTARIO_INICIAL.copy()

consumo_total = {
    "Arroz": 0,
    "Leche": 0,
    "Aceite": 0
}

precios = PRECIOS_INICIALES.copy()


# Creamos la sesión de Spark
spark = (
    SparkSession.builder
    .appName("ConsumidorTienda")
    .master("local[*]")
    .getOrCreate()
)

# Mostramos solo mensajes importantes
spark.sparkContext.setLogLevel("WARN")


# Definimos la estructura de los eventos
schema = """
    producto STRING,
    cantidad INT,
    precio INT,
    precio_anterior INT,
    tipo STRING,
    timestamp STRING
"""


# Leemos los eventos en tiempo real
eventos = (
    spark.readStream
    .schema(schema)
    .json(CARPETA_EVENTOS)
)


# Guardamos el estado actual en un archivo
def guardar_estado(estado):

    archivo_temporal = ARCHIVO_ESTADO + ".tmp"

    with open(
        archivo_temporal,
        "w",
        encoding="utf-8"
    ) as archivo:

        json.dump(
            estado,
            archivo,
            ensure_ascii=False,
            indent=4
        )

    os.replace(
        archivo_temporal,
        ARCHIVO_ESTADO
    )


# Procesamos cada grupo de eventos
def procesar_lote(df, batch_id):

    global inventario
    global consumo_total
    global precios

    # Si no hay datos, no procesamos
    if df.isEmpty():
        return

    print("\n====================================")
    print(f"        BATCH {batch_id}")
    print("====================================")


    # Buscamos los eventos de consumo
    consumos = (
        df.filter(col("tipo") == "CONSUMO")
        .groupBy("producto")
        .agg(
            spark_sum("cantidad").alias("cantidad_consumida")
        )
        .collect()
    )


    # Actualizamos el inventario según el consumo
    for fila in consumos:

        producto = fila["producto"]
        cantidad = int(fila["cantidad_consumida"])

        if producto not in inventario:
            inventario[producto] = 100
            consumo_total[producto] = 0
            precios[producto] = 10000

        inventario[producto] -= cantidad
        consumo_total[producto] += cantidad


    # Buscamos los cambios de precio
    cambios_precio = (
        df.filter(
            col("tipo") == "CAMBIO_PRECIO"
        )
        .select(
            "producto",
            "precio",
            "precio_anterior",
            "timestamp"
        )
        .orderBy("timestamp")
        .collect()
    )


    # Actualizamos los precios
    for fila in cambios_precio:

        producto = fila["producto"]
        nuevo_precio = int(fila["precio"])

        if producto not in precios:
            precios[producto] = nuevo_precio

        precios[producto] = nuevo_precio

        print(
            f"[SPARK] {producto} | "
            f"Cambio de precio: "
            f"${fila['precio_anterior']:,} → "
            f"${nuevo_precio:,}"
        )


    # Creamos el estado de cada producto
    productos_estado = {}

    for producto in inventario:

        stock = inventario[producto]
        total = consumo_total.get(producto, 0)
        precio = precios.get(producto, 0)

        consumo_reciente = 0


        # Buscamos cuánto se consumió recientemente
        for fila in consumos:

            if fila["producto"] == producto:

                consumo_reciente = int(
                    fila["cantidad_consumida"]
                )


        # Calculamos una predicción del inventario
        prediccion = max(
            0,
            stock - (consumo_reciente * 3)
        )


        # Obtenemos el inventario inicial
        inventario_base = INVENTARIO_INICIAL.get(
            producto,
            100
        )

        porcentaje = stock / inventario_base


        # Definimos el estado según el nivel de inventario
        if stock <= 0:

            estado = "AGOTADO"
            alerta = True

        elif porcentaje <= 0.10:

            estado = "CRITICO"
            alerta = True

        elif porcentaje <= 0.25:

            estado = "BAJO"
            alerta = True

        else:

            estado = "NORMAL"
            alerta = False


        # Si el inventario está muy bajo, lo reponemos
        if stock <= inventario_base * 0.10:

            print(
                f"[ALERTA] {producto} tiene "
                f"stock bajo. Reponiendo inventario..."
            )

            inventario[producto] = inventario_base

            stock = inventario[producto]

            estado = "NORMAL"
            alerta = False


        # Guardamos la información del producto
        productos_estado[producto] = {
            "inventario": stock,
            "consumo_total": total,
            "consumo_reciente": consumo_reciente,
            "prediccion": prediccion,
            "precio": precio,
            "estado": estado,
            "alerta": alerta
        }


        # Mostramos el resultado
        print(
            f"[SPARK] {producto} | "
            f"Consumo: {consumo_reciente} | "
            f"Inventario: {stock} | "
            f"Predicción: {prediccion} | "
            f"Precio: ${precio:,} | "
            f"Estado: {estado}"
        )


    # Creamos el estado final
    estado_final = {
        "productos": productos_estado,
        "ultima_actualizacion": __import__(
            "datetime"
        ).datetime.now().isoformat()
    }


    # Guardamos el estado actualizado
    guardar_estado(estado_final)

    print(
        "[SPARK] Estado guardado correctamente."
    )


# Iniciamos el procesamiento continuo de Spark
consulta = (
    eventos.writeStream
    .foreachBatch(procesar_lote)
    .option(
        "checkpointLocation",
        "checkpoint/consumidor"
    )
    .trigger(processingTime="5 seconds")
    .start()
)


# Mostramos que el consumidor está activo
print("====================================")
print("          CONSUMIDOR SPARK")
print("====================================")
print("Esperando eventos...")
print("Presiona CTRL+C para detener.\n")


# Mantenemos el programa ejecutándose
consulta.awaitTermination()