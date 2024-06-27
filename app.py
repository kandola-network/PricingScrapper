import requests
import json
import pandas as pd
import os
import time
from pymongo import UpdateOne, MongoClient

def awsScraper():
  response=requests.get('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/index.json')
  print('Data fetched')
  response=response.json()
  products=pd.DataFrame(response.get('products'))
  products = products.transpose()
  regionCodeNameSlugMap=(["us-gov-east-1", "AWS GovCloud (US-East)", "aws-govcloud-us-east"],
  ["us-gov-west-1", "AWS GovCloud (US-West)", "aws-govcloud-us-west"],
  ["af-south-1", "Africa (Cape Town)", "africa-cape-town"],
  ["ap-east-1", "Asia Pacific (Hong Kong)", "asia-pacific-hong-kong"],
  ["ap-northeast-1", "Asia Pacific (Tokyo)", "asia-pacific-tokyo"],
  ["ap-northeast-2", "Asia Pacific (Seoul)", "asia-pacific-seoul"],
  ["ap-northeast-3", "Asia Pacific (Osaka)", "asia-pacific-osaka"],
  ["ap-south-1", "Asia Pacific (Mumbai)", "asia-pacific-mumbai"],
  ["ap-southeast-1", "Asia Pacific (Singapore)", "asia-pacific-singapore"],
  ["ap-southeast-2", "Asia Pacific (Sydney)", "asia-pacific-sydney"],
  ["ap-southeast-3", "Asia Pacific (Jakarta)", "asia-pacific-jakarta"],
  ["ca-central-1", "Canada (Central)", "canada-central"],
  ["eu-central-1", "Europe (Frankfurt)", "europe-frankfurt"],
  ["eu-north-1", "Europe (Stockholm)", "europe-stockholm"],
  ["eu-south-1", "Europe (Milan)", "europe-milan"],
  ["eu-west-1", "Europe (Ireland)", "europe-ireland"],
  ["eu-west-2", "Europe (London)", "europe-london"],
  ["eu-west-3", "Europe (Paris)", "europe-paris"],
  ["me-south-1", "Middle East (Bahrain)", "middle-east-bahrain"],
  ["sa-east-1", "South America (Sao Paulo)", "south-america-sao-paulo"],
  ["us-east-1", "US East (N. Virginia)", "us-east-n-virginia"],
  ["us-east-2", "US East (Ohio)", "us-east-ohio"],
  ["us-west-1", "US West (N. California)", "us-west-n-california"],
  ["us-west-2", "US West (Oregon)", "us-west-oregon"],
  ["us-west-2-lax-1", "US West (Los Angeles)", "us-west-los-angeles"],
  ["ap-south-2", "Asia Pacific (Hyderabad)", "asia-pacific-hyderabad"],
  ["eu-central-2", "Europe (Zurich)", "europe-zurich"],
  ["eu-south-2", "Europe (Spain)", "europe-spain"],
  ["me-central-1", "Middle East (UAE)", "middle-east-uae"],
  [
    "Asia Pacific (Osaka-Local)",
    "Asia Pacific (Osaka-Local)",
    "asia-pacific-osaka-local",
  ])
  regionCodeSlugMap={}
  rows=[]
  cornVersions=int(time.time())
  for i in regionCodeNameSlugMap:
    regionCodeSlugMap[i[0]]=i[2]
  for index, row in products.loc[:,['sku','productFamily','attributes']].iterrows():
    if row.attributes['locationType']!='AWS Outposts' and regionCodeSlugMap.get(row.attributes['regionCode']):
          updateDoc={
                  "sku": row.sku,
                  "productFamily": row.productFamily,
                  "regionCode":row.attributes['regionCode'],
                  "regionSlug":regionCodeSlugMap[row.attributes['regionCode']],
                  "version": response.get('version'),
                  }
          if row.attributes.get('deploymentOption'):
            updateDoc['deploymentOption']= str(row.attributes['deploymentOption'])
          if row.attributes.get('memory'):
              updateDoc['memory']=float(row.attributes.get('memory').replace(' GiB',''))
          if row.attributes.get('vcpu'):
              updateDoc['vcpu']=int(row.attributes.get('vcpu'))
          if row.attributes.get('databaseEngine')  in ['MariaDB','PostgreSQL','MySQL','Oracle']:
            updateDoc['databaseEngine']= str(row.attributes['databaseEngine']),
            updateDoc['provider']='aws'
            reservations=[]
            offerList=[]
            if row.productFamily == 'Database Storage':
              minStorageAmount, minStorageUnit = row.attributes.get('maxVolumeSize').split(' ')
              maxStorageAmount, maxStorageUnit = row.attributes.get('maxVolumeSize').split(' ')
              if minStorageUnit == 'TB':
                minStorageAmount = int(minStorageAmount) * 1024
              if maxStorageUnit == 'TB':
                maxStorageAmount = int(maxStorageAmount) * 1024
              updateDoc['minVolumeSize']=minStorageAmount
              updateDoc['maxVolumeSize']=maxStorageAmount
              updateDoc['volumeType']=row.attributes.get('volumeType')
              updateDoc['storageMedia']=row.attributes.get('storageMedia')
            if response.get('terms').get('OnDemand').get(row.sku):
              reservations.append('OnDemand')
              for i in response.get('terms').get('OnDemand').get(row.sku).values():
                for j in i['priceDimensions'].values():
                    offerList.append({
                        'reservation':'OnDemand',
                        'unit':j.get('unit'),
                        'pricePerUnit':j.get('pricePerUnit').get('USD'),
                    })
            if response.get('terms').get('Reserved').get(row.sku):
              reservations.append('Reserved')
              for i in response.get('terms').get('Reserved').get('4RPTZZPDNYGAVHMP').values():
                for j in i['priceDimensions'].values():
                    if i.get('termAttributes').get('PurchaseOption') == 'All Upfront' and j.get('unit')=='Quantity':
                        offerList.append({
                            'reservation':'Reserved',
                            'unit':j.get('unit'),
                            'duration': int(i.get('termAttributes').get('LeaseContractLength').split('yr')[0]),
                            'pricePerUnit': j.get('pricePerUnit').get('USD'),
                        })
            updateDoc['offers']=offerList
            updateDoc['reservations']=reservations
            updateDoc['__v']=cornVersions
            rows.append(
              UpdateOne({
                "sku":row.sku
                },{
                "$set": updateDoc},
                                    upsert=True)
                        )
  uri=os.getenv('MONGO_URI')
  client = MongoClient(uri)
  kandola=client['kandola']
  pricing=kandola['pricing']
  pricing.bulk_write(rows)
  print('Data inserted')
  pricing.delete_many({"__v":{"$lt":cornVersions},"provider":"aws"})
  print('Deleted old version data if any')
  return
awsScraper()
print('done')