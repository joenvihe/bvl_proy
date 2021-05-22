# Bolsa de Valores del PERU


## Configuración

Listar entornos:

- conda env list
base                  *  C:\ProgramData\Anaconda3
envDatax                 d:\users\jvicenth\.conda\envs\envDatax

- conda create -n envPy38 python=3.8

- conda activate envPy38

- pip install -r requirement.txt
- pip install -U requirement.txt

## Obtener la lista de codigos de la BVL

Request URL: https://dataondemand.bvl.com.pe/v1/stock-quote/home
Request Method: POST
Status Code: 200 
Remote Address: 13.227.201.80:443
Referrer Policy: strict-origin-when-cross-origin


{sector: "", isToday: true, companyCode: "", inputCompany: ""}
companyCode: ""
inputCompany: ""
isToday: true
sector: ""

## Cuales son las tareas?:
- Extraer data de los códigos de la BVL
- Extraer precios por codigo de los ultimos 10 años, extraer info por semana
- Extraer informacion de las noticias economicas y financiera
- Estructurar la data
- Realizar el analisis con data estructurada la llave seria la fecha