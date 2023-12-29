import requests
import re
import time

order_api = 'https://www-genesis.destatis.de/genesisWS/rest/2020/data/table?username={0}&password={1}&name=46241-0011&area=all&compress=false&transpose=false&startyear=2008&endyear=2023&timeslices=&regionalvariable=&regionalkey=&classifyingvariable1=&classifyingkey1=&classifyingvariable2=&classifyingkey2=&classifyingvariable3=&classifyingkey3=&job=true&language=de'
result_collect_api = 'https://www-genesis.destatis.de/genesisWS/rest/2020/data/result?username={0}&password={1}&name={2}&area=all&compress=false&language=de'

def get_csv_from_genesis(username, password):
    print("Start pulling data from Destatis Genesis. This can take a bit.")
    tablename = _create_order(username, password)
    _collect_table(username, password, tablename)

def _create_order(username, password):
    resp = requests.get(order_api.format(username, password))
    json_res = resp.json()
    matcher = re.search("Der Bearbeitungsauftrag wurde erstellt. Die Tabelle kann in Kürze als Ergebnis mit folgendem Namen abgerufen werden: ((\w|-)+)",
                        json_res['Status']['Content'])
    return matcher.group(1)

def _collect_table(username, password, tablename):
    while True:
        resp = requests.get(result_collect_api.format(username, password, tablename))
        json_res = resp.json()
        if json_res['Status']['Code'] == 0:
            csv = json_res['Object']['Content']
            break
        if json_res['Status']['Code'] != 104:
            raise Exception('Unexpected error code')
        print("Please wait...")
        time.sleep(15)
    print("Got the data. Continuing with transformation...")
    with open('data/accidents.csv', 'w') as csv_file:
        csv_file.write(csv)
