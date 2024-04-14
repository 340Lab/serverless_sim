import requests
from pprint import pprint
import time
import threading
import json
# pa's blog
# GATEWAY='http://hanbaoaaa.xyz/waverless_api1'

# deploy cluster
# GATEWAY='http://192.168.31.162:'

# deploy single node
GATEWAY='http://127.0.0.1:3000'

# APP='fn2'
# APP='longchain'
APP='get_network_topo'

def run_one():
    ms = time.time()*1000.0
    res = requests.post(f'{GATEWAY}/{APP}',json={
        "env_id":"sadsd"
    },headers={
        'Content-Type': 'application/json'
    })

    ms_ret = time.time()*1000.0
    print(res, ms_ret-ms,res.text)

# 10 concurrent requests by multi-threading
for i in range(1):
    threading.Thread(target=run_one).start()