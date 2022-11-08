import argparse
import json
import logging

import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open
import os


business = pd.read_json("/Users/thomaslaurel/Desktop/jump/gcpsolution/yelptestjson.json")

print(business)

'''
with open("/Users/thomaslaurel/Desktop/jump/gcpsolution/yelptestjson.json") as fin:
    ss=fin.read()
    data = json.loads(ss)

    
    product = data.get('product')

    if product and product.get('id'):
        print ("there exists a product")
    '''