import requests
import json

URL = "https://dataondemand.bvl.com.pe/v1/stock-quote/home"
var_payload = {"sector": "", "isToday": "true", "companyCode": "", "inputCompany": ""}



if __name__ == "__main__":
    r = requests.post(URL, data=json.dumps(var_payload))
    lista_codigos = json.loads(r.text)
    print(lista_codigos[0])