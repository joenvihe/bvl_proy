import requests
import json

from requests.api import request
import psycopg2
from dotenv import load_dotenv   #for python-dotenv method
load_dotenv()                    #for python-dotenv method
import os 
from datetime import datetime
import pandas as pd
import sys


URL = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api/{}/json/2000-1-1/2100-1-1"



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

def select_tipo_bcrp():
    query = """
    SELECT periodo,codigo, descripcion
    FROM tipos_bcrp 
    """

    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query) 

    list_stockCode = []
    row = cur.fetchone()
    while row is not None:
        #print(row)
        list_stockCode.append((row[0],row[1],row[2]))
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode

def insert_row_valor_bcrp(lst_row):
    try:
        
        query = """INSERT INTO public.valor_bcrp(codigo,periodo,valor)
	               VALUES {} """.format(lst_row)
        # connect to the PostgreSQL server
        conn = connect_postgres()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        
        count = cur.rowcount
        print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)   


if __name__ == "__main__":
    lst = select_tipo_bcrp()

    for i in lst:
        #print(URL.format(i[1]))
        r = requests.get(URL.format(i[1]))
        vinfo = json.loads(r.text)
        #print(vinfo["periods"])
        reg = ""
        for v in vinfo["periods"]:
            reg += "('{}','{}','{}'),".format(i[1],v["name"],v["values"][0])
        insert_row_valor_bcrp(reg[:-1])
        #break






















