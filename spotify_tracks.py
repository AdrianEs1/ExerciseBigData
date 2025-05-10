#Universidad Nacional Abierta y a Distancia UNAD
#Estudiantes: Adrian Armero, Leonardo Ramirez
#Curso: BIGDATA


import happybase
import pandas as pd

# Conectarse a HBase
connection = happybase.Connection('localhost')
connection.open()

# Definir la tabla
table = connection.table('spotify_tracks')

# Cargar el CSV con pandas
df = pd.read_csv('SpotifyFeatures.csv').head(100)

# Cargar los datos en HBase
for i, row in df.iterrows():
    row_key = str(row['track_id'])  # Usamos el 'track_id' como clave primaria única
    data = {
        b'info:track_name': str( row['track_name']).encode(),
        b'info:artist_name':str( row['artist_name']).encode(),
        b'info:genre': str(row['genre']).encode(),
        b'info:popularity': str(row['popularity']).encode(),
        b'info:danceability': str(row['danceability']).encode(),
    }
    table.put(row_key, data)
print("Datos Cargados correctamente")


# Recorrer las filas y mostrar el nombre de la pista y el artista
for i,( key, data) in enumerate(table.scan()):
    if i==20:
       break
    track_name = data[b'info:track_name'].decode()
    artist_name = data[b'info:artist_name'].decode()
    print(f'track: {track_name},  by artist: {artist_name}')

# Inserción
row_key = '12345'
data = {
    b'info:track_name': b'Nueva Cancion',
    b'info:artist_name': b'Nuevo Artista',
    b'info:genre': b'Pop',
    b'info:popularity': b'75',
    b'info:danceability': b'0.85',
}
table.put(row_key, data)
print("Inserción completada.")

# Actualización
updated_data = {
    b'info:popularity': b'90',
    b'info:genre': b'Electro'
}
table.put(row_key, updated_data)
print("Actualización completada.")

# Eliminación
table.delete(row_key)
print("Eliminación completada.")


#Expliacion Codigo
"""
Este script realiza una serie de operaciones sobre una base de datos HBase 
utilizando Python. Primero, establece una conexión con HBase y accede a una tabla específica llamada spotify_tracks. 
Luego, carga un archivo CSV que contiene información sobre canciones de Spotify
y guarda los primeros 100 registros en la tabla, utilizando el identificador de la canción como clave principal. 
Posteriormente, lee y muestra los primeros 20 registros guardados, mostrando el nombre de la canción y el artista. 
Además, realiza una inserción manual de un nuevo registro, luego lo actualiza y, finalmente, lo elimina. 
Todo el proceso termina cerrando la conexión con la base de datos. 
En conclusión, el código demuestra cómo cargar, consultar, insertar, actualizar y eliminar datos en HBase de manera sencilla con Python.
"""
