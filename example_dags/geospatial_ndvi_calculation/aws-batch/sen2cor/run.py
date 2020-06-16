import sys
import os
import json
import shutil
import datetime
import pickle
from collections import OrderedDict
from pprint import pprint
from storage import COS
from sentinel import get_sentinel_metadata_from_area, jp2_to_cog, download_from_sentinel, get_geojson_info
from sen2cor import perform_atmospheric_corrections, generate_bands, combine_bands

cos = COS(ibm_api_key_id=os.environ['IBM_API_KEY_ID'],
          ibm_service_instance_id=os.environ['IBM_SERVICE_INSTANCE_ID'],
          endpoint_url=os.environ['ENDPOINT_URL'],
          bucket=os.environ['BUCKET'])

key = sys.argv[1]
meta = pickle.loads(cos.get_object(key))

pprint(meta)

products = OrderedDict()
products[meta['uuid']] = meta.copy()

products_geojson = get_geojson_info(products)

if len(products_geojson['features']) != 1:
    raise Exception('Only one tile is permitted')

product_geojson = products_geojson['features'].pop()
product = product_geojson['properties']
tile = product['filename'][39:44]
date = product['filename'][11:19]
exists_geojson = cos.check_pattern(tile, date, 'geojson')
remote_geotiff = cos.get_pattern(tile, date, 'tif', 'COMBINED')

if remote_geotiff is None:

    tmpdir = '.'

    # Download a Sentinel-2 tile
    downloaded_folder = download_from_sentinel(product, tmpdir)

    # Atmospheric correction
    corrected_folder = perform_atmospheric_corrections(product)

    # Translate bands in .jp2 to Cloud Optimized geoTiff format
    files = generate_bands(product)

    # Merge both bands into a single geotiff
    combined_geotiff = combine_bands(files)

    # Upload generated files to IBM Cloud
    meta['combined_geotiff'] = cos.upload_to_ibm_cloud([combined_geotiff], product).pop()

    # Clean corrected folder
    shutil.rmtree(corrected_folder)

    # Clean downloaded folder
    shutil.rmtree(downloaded_folder)

    # Clean optimized files
    [os.remove(file) for file in files]

    # Clean combined file
    os.remove(combined_geotiff)

else:
    meta['combined_geotiff'] = remote_geotiff

if not exists_geojson:
    # Generate and upload geojson file to IBM cloud
    cos.upload_geojson(product_geojson)

pprint(meta)
cos.put_object(key, pickle.dumps(meta))
