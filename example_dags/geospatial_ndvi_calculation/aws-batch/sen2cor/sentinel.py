import tempfile
import zipfile
import subprocess
import os
import glob

import sentinelsat
from storage import COS
from rio_tiler.sentinel2 import _sentinel_parse_scene_id
from rio_cogeo.cogeo import cog_translate


def get_sentinel_metadata_from_area(from_date, to_date, geo_json_area, cloudcoverpercentage=(0, 15)):
    api = sentinelsat.SentinelAPI(user=os.environ["SENTINEL_USERNAME"],
                                  password=os.environ["SENTINEL_PASSWORD"])

    footprint = sentinelsat.geojson_to_wkt(geo_json_area)
    products = api.query(footprint,
                         date=(from_date, to_date),
                         platformname='Sentinel-2',
                         producttype=('S2MS2Ap', 'S2MSI1C'),
                         cloudcoverpercentage=cloudcoverpercentage)
    return products


def jp2_to_cog(band_src_path):
    '''
    Given the path of a band of sentinel (.jp2) generates a Cloud Optimized GeoTiff version.
    '''
    config = dict(NUM_THREADS=100, GDAL_TIFF_OVR_BLOCKSIZE=128)

    output_profile = {
        "driver": "GTiff",
        "interleave": "pixel",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "compress": "DEFLATE",
    }

    cog_path = f"{band_src_path[band_src_path.rfind('/')+1:band_src_path.rfind('.')]}.tif"
    cog_translate(
        band_src_path,
        cog_path,
        output_profile,
        nodata=0,
        in_memory=False,
        config=config,
        quiet=True,
    )
    return cog_path


def download_unzip_transform_to_geotiff(product):
    api = sentinelsat.SentinelAPI(user=os.environ["SENTINEL_USERNAME"],
                                  password=os.environ["SENTINEL_PASSWORD"])

    cos = COS(ibm_api_key_id=os.environ['IBM_API_KEY_ID'],
              ibm_service_instance_id=os.environ['IBM_SERVICE_INSTANCE_ID'],
              endpoint_url=os.environ['ENDPOINT_URL'],
              bucket=os.environ['BUCKET'])
    with tempfile.TemporaryDirectory() as tmpdir:
        # Download a Sentinel-2 tile
        d_meta = api.download(product['uuid'], directory_path=tmpdir)
        # Extract and remove zip file
        zip_ref = zipfile.ZipFile(d_meta['path'])
        zip_ref.extractall(tmpdir)
        zip_ref.close()
        os.remove(d_meta['path'])
        # Atmospheric correction
        sentinel_product_dir = product['filename']
        val = subprocess.check_call(
            f'{os.environ["SEN2COR_COM"]} --resolution 10 {sentinel_product_dir}')
        # Translate bands in .jp2 to Cloud Optimized geoTiff format
        band4 = glob.glob(
            f"{sentinel_product_dir[0:26].replace('1C', '2A')}*/GRANULE/*/IMG_DATA/R10m/*B04*")
        band4 = band4[0] if len(band4) == 1 else None
        band8 = glob.glob(
            f"{sentinel_product_dir[0:26].replace('1C', '2A')}*/GRANULE/*/IMG_DATA/R10m/*B08*")
        band8 = band8[0] if len(band8) == 1 else None
        band4_tiff_file = jp2_to_cog(band4)
        band8_tiff_file = jp2_to_cog(band8)
        cos.upload_band_file(band4_tiff_file, product)
        cos.upload_band_file(band8_tiff_file, product)


def download_from_sentinel(product, tmpdir):
    api = sentinelsat.SentinelAPI(user=os.environ["SENTINEL_USERNAME"],
                                  password=os.environ["SENTINEL_PASSWORD"])

    sentinel_product_dir = product['filename']
    if not os.path.exists(sentinel_product_dir):
        d_meta = api.download(product['uuid'], directory_path=tmpdir)
        # Extract and remove zip file
        zip_ref = zipfile.ZipFile(d_meta['path'])
        zip_ref.extractall(tmpdir)
        zip_ref.close()
        os.remove(d_meta['path'])
    return sentinel_product_dir


def get_geojson_info(products):

    api = sentinelsat.SentinelAPI(user=os.environ["SENTINEL_USERNAME"],
                                  password=os.environ["SENTINEL_PASSWORD"])
    return api.to_geojson(products)
