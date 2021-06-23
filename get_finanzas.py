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

URL_INFO = "https://dataondemand.bvl.com.pe/v1/issuers/{}/info"
URL_VALUE = "https://dataondemand.bvl.com.pe/v1/issuers/{}/value"
URL_COD = "https://dataondemand.bvl.com.pe/v1/financial-statements/{}"
URL = "https://dataondemand.bvl.com.pe/v1/financial-statements/"

#COD = "OE2305"
#COD = "B60051"
COD = "B20003"
YEAR_PERIOD = "2000"


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

def select_companyStock_with_code():
    query = """
    SELECT a."rpjCode",a.companyCode,a.companyName,a.nemonico,a.sectorCode,a.sectorDescription
    FROM companyStock a
    WHERE a."rpjCode" IS NOT NULL
    """

    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query) 

    list_stockCode = []
    row = cur.fetchone()
    while row is not None:
        #print(row)
        list_stockCode.append(row[0])
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode


def select_companyStock(op,codigo = ""):
    if op == 1:
        query = """
        SELECT a.companyCode,a.companyName,a.nemonico,a.sectorCode,a.sectorDescription
        FROM companyStock a
        WHERE a."rpjCode" IS NULL
        """
    elif op == 2:
        query = """
        SELECT a.companyCode,a.companyName,a.nemonico,a.sectorCode,a.sectorDescription
        FROM companyStock a
        WHERE a."rpjCode" IS NOT NULL
        """
    elif op == 3:
        query = """
        select datedelivery from stockcompanyvalue  where codigo = '{}'  order by 1 desc  limit 1
        """.format(codigo)
        print(query)


    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query) 

    list_stockCode = []
    row = cur.fetchone()
    while row is not None:
        #print(row)
        list_stockCode.append(row[0])
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode

def update_row_companyStock(code,website,desc,compcode):
    try:
        query = """
                UPDATE companyStock
                SET "rpjCode" = '{}',
                    website = '{}',
                    "esActDescription" = '{}'
                WHERE companyCode = '{}' """.format(code,website,desc,compcode)
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


def insert_row_stockvalues(lst_row):
    try:
        
        query = """INSERT INTO public.stockcompanyvalue(codigo,nemonico,benefitValue,benefitType,isin,dateEntry,dateAgreement,
                                                        dateCut,dateRegistry,dateDelivery,coin,secMovBe,secMovDi,notesValue,
                                                        notesLaw,notesAgreement,notesCut,notesRegistry,notesDelivery)
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

    par = sys.argv # 1 el año
    # parametro 1 y 2:
    # 1 actualizar el update del codigo
    # 2 2000 , dos variables 

    
    lst_comp = select_companyStock(1)
    if par[1] == "1":
        for codigo in lst_comp:
            if codigo != "XXX":
                r = requests.get(URL_INFO.format(codigo))
                vinfo = json.loads(r.text)
                update_row_companyStock(vinfo["rpjCode"],vinfo["website"],vinfo["esActDescription"],codigo) 

    lst_comp = select_companyStock(2)
    if par[1] == "2":
        for codigo in lst_comp:
            r = requests.get(URL_VALUE.format(codigo))
            vinfo = json.loads(r.text)

            ldate = select_companyStock(3,codigo)
            val  = ""
            entro = False
            for v in vinfo[0]["listBenefit"]:
                try:
                    if len(ldate) > 0 and v["dateDelivery"] > ldate[0] and str(v["dateDelivery"]) != str(ldate[0]):
                        entro = True  
                        val += "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),".format(codigo,v['nemonico'],v['benefitValue'],v['benefitType'],v['isin'],v['dateEntry'],v['dateAgreement'],v['dateCut'],v['dateRegistry'],v['dateDelivery'],v['coin'],v['secMovBe'],v['secMovDi'],v['notesValue'],v['notesLaw'],v['notesAgreement'],v['notesCut'],v['notesRegistry'],v['notesDelivery'])
                    elif len(ldate) == 0:
                        entro = True
                        val += "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),".format(codigo,v['nemonico'],v['benefitValue'],v['benefitType'],v['isin'],v['dateEntry'],v['dateAgreement'],v['dateCut'],v['dateRegistry'],v['dateDelivery'],v['coin'],v['secMovBe'],v['secMovDi'],v['notesValue'],v['notesLaw'],v['notesAgreement'],v['notesCut'],v['notesRegistry'],v['notesDelivery'])
                except Exception as e:
                    entro = False
                
            if entro:
                insert_row_stockvalues(val[:-1])
            #break
            
    lst_code = select_companyStock_with_code()
    if par[1] == "3":
        arrV = []
        for codigo in lst_code:
            r = requests.get(URL_COD.format(codigo))
            print(URL_COD.format(codigo))
            data = json.loads(r.text)

            # variables generales        
            for v in data:
                for i in v["finantialIndexYears"]:
                    if (int(par[2])-1) == int(i["year"]):
                        print(v["dRatio"])
                        print(i["year"])
                        print(i["nImporteA"])


            #df = pd.DataFrame.from_dict(data, orient='columns')
            #print(df)

            # variables detalle
            payload = {
                "page": "1",
                "period": "1",
                "periodAccount": "",
                "rpjCode": codigo,
                "search": "",
                "size": "12",
                "type": "1",
                "yearPeriod": par[2]
            }

            r = requests.post(URL, json=payload)
            lista_values = json.loads(r.text)

            #df = pd.DataFrame.from_dict(lista_values, orient='columns')
            #print(r.status_code)
            #print(df)
            print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            for v in lista_values["content"]:
                #print(v["document"])
                if "document" in v:
                    #print(v["document"])
                    for i in v["document"]:
                        print(v["yearPeriod"])
                        print(v["period"])
                        print(v["documentName"])
                        print(v["documentOrder"])
                        print(v["path"])
                        print(v["quantityParts"])
                        print(v["eeffType"])
                        print(i["caccount"])
                        print(i["mainTitle"])
                        print(i['numberColumns'])
                        print(i['title'])
                        print(i['value1'])
                        print(i['value2'])
                        print(i['value3'])
                        print(i['value4'])
                        print(i['value5'])
                        print(i['value6'])
                        print(i['value7'])
                        print(i['value8'])
                        print(i['value9'])
                        print(i['value10'])
                        print(i['value11'])
                        print(i['value12'])
                        print(i['value13'])
                        print(i['value14'])
                        print(i['value15'])
                        print(i['value16'])
                        print(i['value17'])
                        print(i['value18'])
                        print(i['value19'])
                        print(i['value20'])
                        print(i['value21'])
                        print(i['value22'])
                        print(i['value23'])
                        print(i['value24'])
                        print(i['value25'])

            break
