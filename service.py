# -*- coding: utf-8 -*-
from forecaster.read_from_s3 import pd_read_csv_s3
from forecaster.forecast import forecast_total
import os
import json
import boto3


def retail_prophet():
    json.load_s3 = lambda f: json.load(s3.Object(key=f).get()["Body"])
    json.dump_s3 = lambda obj, f: s3.Object(key=f).put(Body=json.dumps(obj))
    try:
        lastrows = int(os.getenv('LASTROWS'))
        print(lastrows)
    except:
        lastrows = None
    try:
        canales = os.getenv('CANALES').split(',')
        print(canales)
    except:
        canales = None
    cache = os.getenv('CACHE')
    if cache:
        if cache == 'False':
            cache = False
        elif cache == 'True':
            cache = True
    else:
        cache = False
    print("CACHE IS " + str(cache))
    bucket = os.getenv('BUCKET')
    key = os.getenv('KEY')
    path = '/'.join([bucket, key])
    df = pd_read_csv_s3(path, compression="gzip")
    print(df)
    if df is None:
        print("Can't read file")
        return {}
    if canales is None or canales == 'All':
        canales = ["directo", "google", "google seo", "mailing", "newsroom",
                   "facebook", "referrers",
                   "paid_social_samsung", "totales"]
    result = {}
    s3 = boto3.resource("s3").Bucket(bucket)
    if cache:
        try:
            print("Entered first try")
            body = json.load_s3("prophet.json")
            response = {
                "statusCode": 200,
                "body": json.dumps(body)
            }
            return response
        except:
            print("Entered except")
            response = {
                "statusCode": 404,
                "error": ("there is no previous prophet result, please run"
                          "without cache")
            }
            return response
    else:
        result = {}
        print("Entered success:")
        for canal in canales:
            if lastrows:
                if lastrows != 'All':
                    canal_df = df[['fecha', canal]].tail(lastrows)
            canal_df = df[['fecha', canal]]
            result.update({canal: forecast_total(canal_df)})
        response = {
            "statusCode": 200,
            "body": json.dumps(result)
        }
        json.dump_s3(result, "prophet.json")


retail_prophet()
