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

- Se intalao Heroku Cli
>heroku login
>heroku addons --all
>heroku addons:info postgresql-flexible-84171
>heroku addons:plans heroku-postgresql
    - heroku-postgresql:hobby-basic
>heroku pg:info --app compra-ahorro
- cambiar de plan
>heroku addons:create heroku-postgresql:hobby-basic --app compxxxxxxxxxrro
>heroku maintenance:on --app compxxxxxxxxxrro
>heroku pg:copy DATABASE_URL HEROKU_POSTGRESQL_XXXX_URL --app compxxxxxxxxxrro
>heroku pg:promote HEROKU_POSTGRESQL_XXXX --app compxxxxxxxxxrro
>heroku maintenance:off --app compra-ahorro

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

## Obtener noticias del periodico de la republica por API's
https://larepublica.pe/pf/api/v3/content/fetch/secciones?query=%7B%22from%22%3A25%2C%22seccion%22%3A%22economia%22%2C%22size%22%3A10%7D&d=1439&_website=gruporepublica
https://larepublica.pe/pf/api/v3/content/fetch/secciones?query=%7B%22from%22%3A36%2C%22seccion%22%3A%22economia%22%2C%22size%22%3A10%7D&d=1439&_website=gruporepublica

{"from":36,"seccion":"economia","size":10}&d=1439&_website=gruporepublica

https://larepublica.pe/pf/api/v3/content/fetch/listItemSchema?query=%7B%22from%22%3A%220%22%2C%22seccion%22%3A%22economia%22%2C%22size%22%3A%22100%22%7D&d=1416&_website=gruporepublica

{"from":"0","seccion":"economia","size":"100"}&d=1416&_website=gruporepublica

## Cuales son las tareas?:
- Extraer data de los códigos de la BVL
- Extraer precios por codigo de los ultimos 10 años, extraer info por semana
- Extraer informacion de las noticias economicas y financiera
- Estructurar la data
- Realizar el analisis con data estructurada la llave seria la fecha




## Inversiones
- valor historico de dividendos
- calidad de equipo diretivo
- prsptivas y plans a largo plazo
- fortalzas financiras
- taza actual de dividendos

- Tamaño adcuuado de la mprsa = #acciones * prcio d accion
    - buscar indics slctivos S6P pru 
- estado financiro solicdo
-  estabilidad del benficio
- historia del dividendos
- creciminto del benficio
- PER modrado
- ratio moderado de precio aactivos

## activar mis enviroment en anaconda jupyter notebook

(base)c:> conda install -c anaconda ipykernel
(base)c:> python -m ipykernel install --user --name=envPy38

