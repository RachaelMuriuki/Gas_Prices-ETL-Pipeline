import requests
import os
import http.client 

def extract_data():
    conn = http.client.HTTPSConnection("api.collectapi.com")

    headers = {
         'content-type': "application/json",
         'authorization': os.getenv("GAS_API_KEY")
        }

    conn.request("GET", "/gasPrice/stateUsaPrice?state=WA", headers=headers)

    res = conn.getresponse()
    data = res.read()

    return data