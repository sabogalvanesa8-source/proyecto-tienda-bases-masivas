import json
import os
import random
import time
from datetime import datetime


# Definimos dónde se guardan los eventos
CARPETA_EVENTOS = "data/eventos"


# Lista de productos disponibles
PRODUCTOS = [
    "Arroz",
    "Leche",
    "Aceite"
]


# Precios iniciales de los productos
PRECIOS = {
    "Arroz": 10000,
    "Leche": 8000,
    "Aceite": 12000
}


# Cantidad de eventos que tendrá cada archivo
EVENTOS_POR_ARCHIVO = 5

# Tiempo entre cada evento
INTERVALO_EVENTO = 2


# Creamos la carpeta si no existe
os.makedirs(CARPETA_EVENTOS, exist_ok=True)


# Generamos un evento aleatorio
def generar_evento():

    producto = random.choice(PRODUCTOS)

    # Elegimos entre consumo o cambio de precio
    tipo_evento = random.choices(
        ["CONSUMO", "CAMBIO_PRECIO"],
        weights=[80, 20],
        k=1
    )[0]


    # Generamos un evento de consumo
    if tipo_evento == "CONSUMO":

        cantidad = random.randint(1, 5)

        evento = {
            "producto": producto,
            "cantidad": cantidad,
            "precio": PRECIOS[producto],
            "precio_anterior": None,
            "tipo": "CONSUMO",
            "timestamp": datetime.now().isoformat()
        }


    # Generamos un cambio de precio
    else:

        precio_anterior = PRECIOS[producto]

        # El precio cambia entre -10% y +10%
        variacion = random.uniform(-0.10, 0.10)

        nuevo_precio = int(
            precio_anterior * (1 + variacion)
        )

        # Evitamos precios demasiado bajos
        nuevo_precio = max(1000, nuevo_precio)

        PRECIOS[producto] = nuevo_precio

        evento = {
            "producto": producto,
            "cantidad": 0,
            "precio": nuevo_precio,
            "precio_anterior": precio_anterior,
            "tipo": "CAMBIO_PRECIO",
            "timestamp": datetime.now().isoformat()
        }

    return evento


# Guardamos varios eventos en un archivo
def guardar_lote(eventos):

    nombre_archivo = (
        f"{CARPETA_EVENTOS}/"
        f"lote_{int(time.time() * 1000)}.json"
    )

    archivo_temporal = nombre_archivo + ".tmp"


    # Escribimos cada evento en una línea
    with open(
        archivo_temporal,
        "w",
        encoding="utf-8"
    ) as archivo:

        for evento in eventos:

            archivo.write(
                json.dumps(
                    evento,
                    ensure_ascii=False
                ) + "\n"
            )


    # Guardamos el archivo definitivo
    os.replace(
        archivo_temporal,
        nombre_archivo
    )

    return nombre_archivo


# Mostramos que el productor está activo
print("====================================")
print("       PRODUCTOR DE EVENTOS")
print("====================================")
print("Generando eventos de consumo y cambios de precio...")
print(f"Se generan {EVENTOS_POR_ARCHIVO} eventos por archivo.")
print("Presiona CTRL+C para detener.\n")


# Lista donde acumulamos los eventos
eventos_pendientes = []


try:

    # Generamos eventos continuamente
    while True:

        evento = generar_evento()

        eventos_pendientes.append(evento)


        # Mostramos los consumos
        if evento["tipo"] == "CONSUMO":

            print(
                f"[PRODUCTOR] "
                f"{evento['producto']} | "
                f"Consumo: {evento['cantidad']} | "
                f"Precio: ${evento['precio']:,}"
            )


        # Mostramos los cambios de precio
        else:

            print(
                f"[PRODUCTOR] "
                f"{evento['producto']} | "
                f"CAMBIO DE PRECIO | "
                f"${evento['precio_anterior']:,} → "
                f"${evento['precio']:,}"
            )


        # Cuando hay 5 eventos, creamos un archivo
        if len(eventos_pendientes) >= EVENTOS_POR_ARCHIVO:

            archivo = guardar_lote(
                eventos_pendientes
            )

            print(
                f"[PRODUCTOR] "
                f"Lote enviado a Spark: "
                f"{len(eventos_pendientes)} eventos"
            )

            print(
                f"[PRODUCTOR] Archivo: {archivo}\n"
            )

            eventos_pendientes = []


        # Esperamos antes de generar otro evento
        time.sleep(INTERVALO_EVENTO)


# Permitimos detener el programa con CTRL+C
except KeyboardInterrupt:

    # Guardamos los eventos que hayan quedado pendientes
    if eventos_pendientes:

        guardar_lote(
            eventos_pendientes
        )

        print(
            f"\n[PRODUCTOR] "
            f"Último lote guardado: "
            f"{len(eventos_pendientes)} eventos"
        )

    print(
        "\n[PRODUCTOR] Productor detenido."
    )