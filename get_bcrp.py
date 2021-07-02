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


URL = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api/{}/json/{}/2100-1-1"



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


def select_last_register(codigo = ""):
    print(codigo)
    query = """
    
select a.codigo, a.periodo , a.valor, CAST(CAST(a.fecha AS DATE) AS varchar) fecha
from
(
select  codigo, periodo, valor,
	CASE 
	WHEN length(periodo) = 8 THEN TO_DATE(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(periodo, 'Sep', '01.09'), 'Ago', '01.08'), 'Jul', '01.07'), 'Jun', '01.06'), 'May', '01.05'), 'Abr', '01.04'), 'Mar', '01.03'), 'Feb', '01.02'), 'Ene', '01.01'), 'Dic', '01.12'), 'Nov', '01.11'), 'Oct', '01.10'), 'DD.MM.YY') + interval '1 month'
	WHEN length(periodo) = 5 THEN TO_DATE(replace(replace(replace(replace(periodo, 'T1', '01.01'), 'T2', '01.04'), 'T3', '01.07'), 'T4', '01.10'),'DD.MM.YY') + interval '3 month'
	ELSE TO_DATE(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(periodo, 'Set', 'September'), 'Ago', 'August'), 'Jul', 'July'), 'Jun', 'June'), 'May', 'May'), 'Abr', 'April'), 'Mar', 'March'), 'Feb', 'February'), 'Ene', 'January'), 'Dic', 'December'), 'Nov', 'November'), 'Oct', 'October'), 'dd.Month.YY')+1 
	END AS fecha
-- select *  --,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(periodo, 'Sep', '01.09'), 'Ago', '01.08'), 'Jul', '01.07'), 'Jun', '01.06'), 'May', '01.05'), 'Abr', '01.04'), 'Mar', '01.03'), 'Feb', '01.02'), 'Ene', '01.01'), 'Dic', '01.12'), 'Nov', '01.11'), 'Oct', '01.10')
from valor_bcrp 
-- where codigo = 'PN01510BM' -- and periodo Like '%Oct%'
-- 	where codigo = 'PD38026MD'
where codigo = '{}'
order by 4 desc
limit 1
) a

    """.format(codigo)

    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query) 

    list_stockCode = []
    row = cur.fetchone()
    while row is not None:
        #print(row)
        list_stockCode.append(row[3])
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode


if __name__ == "__main__":
    lst = select_tipo_bcrp()

    for i in lst:
        #print(URL.format(i[1]))
        fecha = select_last_register(i[1])
        vUrl = URL.format(i[1],fecha[0])
        r = requests.get(vUrl)
        print(vUrl)

        vinfo = json.loads(r.text)
        print(vinfo["periods"])
        #break

        reg = ""
        for v in vinfo["periods"]:
            reg += "('{}','{}','{}'),".format(i[1],v["name"],v["values"][0])
        
        if len(reg)>0:
            insert_row_valor_bcrp(reg[:-1])
        #break






















