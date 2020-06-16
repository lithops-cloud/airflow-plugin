import re
import os
import ibm_boto3
import ibm_botocore
from ibm_botocore.credentials import DefaultTokenManager
from ibm_botocore.client import ClientError
from geojson import Feature, FeatureCollection, dump



class COS:
    def __init__(self, ibm_api_key_id, ibm_service_instance_id, endpoint_url, bucket):
        client_config = ibm_botocore.client.Config(signature_version='oauth',
                                                   max_pool_connections=128)

        self.cos = ibm_boto3.client('s3',
                                    ibm_api_key_id=ibm_api_key_id,
                                    ibm_service_instance_id=ibm_service_instance_id,
                                    config=client_config,
                                    endpoint_url=endpoint_url)
        self.bucket = bucket
    
    def get_object(self, key):
        res = self.cos.get_object(Bucket=self.bucket, Key=key)
        return res['Body'].read()
    
    def put_object(self, key, obj):
        res = self.cos.put_object(Bucket=self.bucket, Key=key, Body=obj)

    def get_cos_files(self):
        res = self.cos.list_objects(Bucket=self.bucket)
        return [obj['Key'] for obj in res['Contents']]


    def multi_part_upload(self, item_name, file_path, extra_args=None):
        part_size = 1024 * 1024 * 5
        file_threshold = 1024 * 1024 * 15
        transfer_config = ibm_boto3.s3.transfer.TransferConfig(
            multipart_threshold=file_threshold,
            multipart_chunksize=part_size
        )
        self.cos.upload_file(Filename=file_path, Bucket=self.bucket, Key=item_name, Config=transfer_config)

    def upload_band_file(self, band_tiff_file, product_meta_data={}):
        p = product_meta_data.copy()
        tile_id = band_tiff_file[1:6]
        band = band_tiff_file[23:26]
        date = band_tiff_file[7:15]
        item_name = f"{date}-{tile_id}-{band_tiff_file}"

        if 'gmlfootprint' in product_meta_data:
            del p['gmlfootprint']

        p['band'] = band
        # Prevent 'int' object has no attribute 'encode' error
        for key in p:
            p[key] = str(p[key])

        meta_data = {
            "ACL": "public-read",
            "Metadata": p,
            "ContentType": "image/tiff",
        }

        return self.multi_part_upload(item_name, band_tiff_file, extra_args=meta_data)
    
    def check_file(self, item_name):
        try:
            self.cos.head_object(Bucket=self.bucket, Key=item_name)
        except Exception:
            return False

    def upload_to_ibm_cloud(self, files, product):
        uploaded_files = []
        for file in files:
            tile_id = file[1:6]
            date = file[7:15]
            item_name = f"{date}-{tile_id}-{file}"
            uploaded_files.append((self.bucket, item_name))
            if not self.check_file(item_name):
                self.upload_band_file(file, product)
        return uploaded_files
    
    def upload_geojson_file(self, geojson_file, product_meta_data):
        p = product_meta_data.copy()
        item_name = f"{geojson_file}"

        if 'gmlfootprint' in product_meta_data:
            del p['gmlfootprint']

        # Prevent 'int' object has no attribute 'encode' error
        for key in p:
            p[key] = str(p[key])

        meta_data = {
            "ACL": "public-read",
            "Metadata": p,
            "ContentType": "application/json",
        }

        return self.multi_part_upload(item_name, geojson_file, extra_args=meta_data)
    
    def upload_geojson(self, product_geojson):
        product = product_geojson['properties']
        filename = f"{product['filename'][:-5]}.geojson"
        # date = filename[11:19]
        # tile = filename[39:44]
        if not self.check_file(filename):
            features = []
            features.append(product_geojson)
            feature_collection = FeatureCollection(features)
            with open(filename, "w") as file:
                dump(feature_collection, file)

            self.upload_geojson_file(filename, product)
            os.remove(filename)

    def check_pattern(self, tile, date, extension, band=None):
        try:
            files = self.get_cos_files()
            pattern = r".*" + \
                re.escape(date) + \
                ".*" + \
                re.escape(tile) + \
                ("" if band is None else ".*" + re.escape(band)) + \
                ".*\." + \
                re.escape(extension)
            filtered = [file for file in files if re.search(pattern, file)]
            return len(filtered) > 0
        except ClientError as be:
            print("CLIENT ERROR: {0}\n".format(be))
        except Exception as e:
            print("Unable to check file existance: {0}".format(e))

    def get_pattern(self, tile, date, extension, band=None):
        try:
            files = self.get_cos_files()
            pattern = r".*" + \
                re.escape(date) + \
                ".*" + \
                re.escape(tile) + \
                ("" if band is None else ".*" + re.escape(band)) + \
                ".*\." + \
                re.escape(extension)
            filtered = [file for file in files if re.search(pattern, file)]
            return filtered[0] if len(filtered) > 0 else None
        except ClientError as be:
            print("CLIENT ERROR: {0}\n".format(be))
        except Exception as e:
            print("Unable to check file existance: {0}".format(e))
