import requests
import hashlib
from time import time
import pandas as pd
import sqlite3
import json

conec = sqlite3.connect('Base')
i = 0
Paises = {}
Dregiones = {}
a = 0
b = 0

url = "https://restcountries-v1.p.rapidapi.com/all"
headers = {
    'x-rapidapi-host': "restcountries-v1.p.rapidapi.com",
    'x-rapidapi-key': "f4583719c1msh1c3308f703b56f4p17fd9djsna261a57ef8d2"
    }
url2 = "https://restcountries.eu/rest/v2/all"

response = requests.request("GET", url, headers=headers)
response = response.json()
response2 = requests.request("GET", url2)
response2 = response2.json()

tiempo_inicial = time()

for resp in response:
    Dregiones[i] = resp["region"]
    i += 1

for resp in response2:
    Paises[b] = {"region": resp["region"], "pais": resp["name"], "idioma": resp["languages"][0]["name"]}
    b += 1

Regiones = {}

# Punto 1 tomar las regiones exitentes
for key, value in Dregiones.items():
    if value not in Regiones.values():
        if value != "":
            Regiones[a] = value
        a += 1

# 2, 3 y 4 punto tomar la informacion de acuerdo a la region tomada en el anterior punto y el tiempo que se demoro en
# cada columna

Resultado = {"region": [], "pais": [], "idioma": [], "time": []}

for key, value in Paises.items():
    if value['region'] not in Resultado["region"]:
        if value['region'] in Regiones.values():
            Resultado["region"].append(value['region'])
            Resultado["pais"].append(value["pais"])
            Resultado["idioma"].append(hashlib.sha1((value['idioma']).encode('utf-8')).hexdigest())
            tiempo_final = time()
            Resultado["time"].append(tiempo_final-tiempo_inicial)

# 5. La tabla debe ser creada en un DataFrame con la libreria PANDAS
df = pd.DataFrame(Resultado)
print("****DataFrame: \n", df)

# 6. Con funciones de la libreria pandas muestre el tiempo total, el tiempo promedio, el tiempo minimo y
# el maximo que tardo en procesar toda las filas de la tabla.
print("Tiempo total", df.get("time").sum())
print("Tiempo Promedio", df.get("time").mean())
print("Tiempo menor", df.get("time").min())
print("Tiempo mayor", df.get("time").max())

# 7. Guarde el resultado en sqlite.
df.to_sql("Base", conec, if_exists='replace', index=False)
base = pd.read_sql('select * from Base', conec)
print("***** Archivo SQLite: \n", base)

# 8. Genere un Json de la tabla creada y guardelo como data.json
result = df.to_json("data.json", orient="table")
with open('data.json') as file:
    archivo = json.load(file)
print("***** Json: ", archivo)




