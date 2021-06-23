import requests
import time
import json
import psycopg2
from dotenv import load_dotenv   #for python-dotenv method
load_dotenv()                    #for python-dotenv method
import os 
from datetime import datetime

NUM_D = "1462" # "1457" #1453

#https://larepublica.pe/pf/api/v3/content/fetch/secciones?query=%7B%22from%22%3A25%2C%22seccion%22%3A%22economia%22%2C%22size%22%3A10%7D&d=1439&_website=gruporepublica
URL = "https://larepublica.pe/pf/api/v3/content/fetch/secciones?query=%7B%22from%22%3A{}%2C%22seccion%22%3A%22{}%22%2C%22size%22%3A{}%7D&d=" + NUM_D + "&_website=gruporepublica"
SECCIONES = ["politica","economia"]

db_host = os.environ.get('HOST')
db_database = os.environ.get('DATABASE')
db_user = os.environ.get('USER')
db_port = os.environ.get('PORT')
db_password = os.environ.get('PASSWORD')
db_uri = os.environ.get('URI')

def connect_postgres():
    conn = psycopg2.connect(
    host=db_host,
    database=db_database,
    user=db_user,
    password=db_password)
    return conn

def insert_row_noticias(lst_row):
    try: 
        
        query = """INSERT INTO noticias (periodico,seccion,_id,canonical_url,display_date,headlines_basic,subheadlines_basic,taxonomy_seo_keywords,taxonomy_tags,_type)
                VALUES {} """.format(lst_row)
        # connect to the PostgreSQL server
        conn = connect_postgres()
        cur = conn.cursor()
        
        #print(query)
        
        
        cur.execute(query)
        conn.commit()
        count = cur.rowcount
        print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)     


if __name__ == "__main__": #96849-96850  - 96986
    inicio = 197600
    size = 100
    count = 500000 # es como un 1er contador de ahi cambia
    
    for seccion in SECCIONES:
        print(seccion)
        while(inicio<count):
            rows_str_slt = ""    
            url_news = URL.format(str(inicio),seccion,size)
            print(url_news)
            ingreso = False
            while not ingreso:
                session = requests.session()
                r = session.get(url_news, headers = {'User-agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'})
                try:
                    lista_codigos1 = json.loads(r.text)
                    ingreso = True
                except Exception as e:
                    print(e)
                    print(len(r.text))
                    print(r.text)
                    print(r.status_code)
                    time.sleep(30)
            
            #print(len(lista_codigos1["content_elements"]))
            if "next" in lista_codigos1:
                inicio = int(lista_codigos1["next"])
            else:
                inicio = 0
                break
            count = int(lista_codigos1["count"])
            print("ini:{}   count:{}".format(inicio,count))
            
            for d in lista_codigos1["content_elements"]:
                v1 = ""
                v2 = ""
                v3 = ""
                v4 = ""
                v5 = ""
                v6 = ""
                v7 = ""
                if "canonical_url" in d:
                    v1 = str(d["canonical_url"]).replace("'","")
                if "display_date" in d:
                    v2 = str(d["display_date"]).replace("'","")
                if "basic" in d["headlines"]:
                    v3 = str(d["headlines"]["basic"]).replace("'","")
                if "subheadlines" in d and "basic" in d["subheadlines"]:
                    v4 = str(d["subheadlines"]["basic"]).replace("'","")
                if "seo_keywords" in d["taxonomy"]:
                    v5 = str(d["taxonomy"]["seo_keywords"]).replace("'","")
                if "tags" in d["taxonomy"]:
                    v6 = str(d["taxonomy"]["tags"]).replace("'","")
                if "type" in d:
                    v7 = str(d["type"]).replace('"','')
                
                row_str = "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),".format("larepublica",seccion,d["_id"],v1,v2,v3,v4,v5,v6,v7)
                rows_str_slt += row_str
                
            insert_row_noticias(rows_str_slt[:-1])
        break