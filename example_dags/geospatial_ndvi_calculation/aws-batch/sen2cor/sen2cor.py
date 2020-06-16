import glob
from sentinel import jp2_to_cog
import os
import rasterio
import subprocess

def perform_atmospheric_corrections(product):
    sentinel_product_dir = product['filename']
    date = product['filename'][11:19]
    tile = product['filename'][39:44]
    corrected_images = glob.glob(f"*2A_{date}*_T{tile}_*.SAFE/GRANULE/*/IMG_DATA/R10m/*B0[48]*.jp2")
    atmospheric_corrected = corrected_images[0] if len(corrected_images) > 0 else None
    # Atmospheric correction
    if (not atmospheric_corrected):  
        print(f'Doing the atmospheric correction for {sentinel_product_dir}')
        val = subprocess.check_call(f'{os.environ["SEN2COR_COM"]} --resolution 10 {sentinel_product_dir}', shell=True)
        corrected_images = glob.glob(f"*2A_{date}*_T{tile}_*.SAFE/GRANULE/*/IMG_DATA/R10m/*B0[48]*.jp2")
        print(f'Atmospheric correction finished {val}')
    corrected_folder = corrected_images[0][:corrected_images[0].index('.SAFE')+len('.SAFE')]
    return corrected_folder

def generate_bands(product):
    files = []
    sentinel_product_dir = product['filename']
    # Translate bands in .jp2 to Cloud Optimized geoTiff format
    date = sentinel_product_dir[11:19]
    tile = sentinel_product_dir[39:44]
    band4 = glob.glob(f"*L2A_{date}*_T{tile}*.SAFE/GRANULE/*/IMG_DATA/R10m/*B04*")
    band4 = band4[0] if len(band4) == 1 else None
    band8 = glob.glob(f"*L2A_{date}*_T{tile}*.SAFE/GRANULE/*/IMG_DATA/R10m/*B08*")
    band8 = band8[0] if len(band8) == 1 else None
    if band4 is not None and band8 is not None:
        band4_tiff_file = f"{band4[band4.rfind('/')+1:band4.rfind('.')]}.tif"
        band8_tiff_file = f"{band8[band8.rfind('/') + 1:band8.rfind('.')]}.tif"
        jp2_to_cog(band4)
        jp2_to_cog(band8)
        files.append(band4_tiff_file)
        files.append(band8_tiff_file)

    return files

def combine_bands(band_files):

    if len(band_files) <= 1:
        raise Exception('Invalid number of files')

    filename = band_files[0][0:22] + '_COMBINED.tif'
    if not os.path.exists(filename):
        # Get metadata from first file
        with rasterio.open(band_files[0]) as src:
            profile = src.profile
            profile.update(count=len(band_files))


        with rasterio.open(filename, 'w', **profile) as dst:
            for i in range(len(band_files)):
                with rasterio.open(band_files[i]) as src:
                    dst.write(src.read(1), i + 1)

    return filename


