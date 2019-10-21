import pymssql  
import json
import datetime
import numpy as np
import requests
import pandas as pd

def sqlcomponent(Server,Username,Password):
    cnxn = pymssql.connect(server=Server, user=Username, password=Password, database='Fulfillment')  
    cursor = cnxn.cursor()  
    cursor.execute('select top(1000000) S.ClientID,S.ShipmentID,S.ShipmentOrderTimestamp,S.ShipmentFirstName,S.ShipmentLastName,S.ShipmentAddress1,'
                   +'S.ShipmentCity,S.ShipmentState,S.ShipmentZip,S.ShipmentCountry,'
                   +'P.ProductSKU,P.ProductDescription,SD.ShipmentDetailQty,S.ShipmentStatus'
                   +' from Shipment S join ShipmentDetail SD on S.ShipmentID = SD.ShipmentID join Product P on SD.ProductID = P.ProductID'
                   +' where S.ShipmentStatus = \'SHIPPED\'and s.ShipmentOrderTimestamp > (select LastExtractDate from ETLExtractLog where JobName=\'ConsumerConnectForReturns\')')
    results = cursor.fetchall()
    print(results)
    results=pd.Series(results)
    return results

def jsonconverter(sqldata):
    check=[]
    data=[]
    cb = {}
    for i in range(len(results)):
        if sqldata[i][1] in check:
            products = {"product":{"sku":str(sqldata[i][10]),"description":str(sqldata[i][11])},"quantity":(sqldata[i][12])}
            cb.get('items').append(products)
        else:
            cb ={"merchantId":str(sqldata[i][0]),"merchantReferenceNumber":str(sqldata[i][1]),"Kvp":{},"orderDate":str(sqldata[i][2]),"shipTo":{"FirstName":str(sqldata[i][3]),"LastName":str(sqldata[i][4]),"mailingAddress":{"address1":str(sqldata[i][5]),"city":str(sqldata[i][6]),"stateOrProvince":str(sqldata[i][7]),"postalCode":str(sqldata[i][8]),"country":str(sqldata[i][9])}},"items":[{"product":{"sku":str(sqldata[i][10]),"description":str(sqldata[i][11])},"quantity":(sqldata[i][12])}]}
            check.append(sqldata[i][1])
            data.append(cb)
    data=pd.Series(data)
    return data



def kafkabody(convertedjson):
    headerss={}
    headerss['Content-Type'] = 'application/vnd.kafka.json.v2+json'
    createURL='https://kafkarest-usea1-nonprod.fdr.pitneycloud.com/topics/fdr-rtrn-int-orders'
    kafkadata={"records": [{"key": "tothecity", "value": {"FullTypeName": "Newgistics.Order.Dto.CreateOrderMessage, Newgistics.Order.Dto, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null","SerialType":convertedjson}}]} 
    json_data=json.dumps(kafkadata)
    create = requests.post(createURL,headers=headerss,data=json_data)
    return  create.status_code