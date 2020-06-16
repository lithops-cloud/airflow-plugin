def map_ndvi(metadata_key, bucket, ibm_cos):
    import pickle
    import numpy as np
    import rasterio

    res = ibm_cos.get_object(Bucket=bucket, Key=metadata_key)
    metadata = pickle.loads(res['Body'].read())
    _, item = metadata['combined_geotiff']
    dat = ibm_cos.get_object(Bucket=bucket, Key=item)['Body']

    with rasterio.open(dat) as src:
        profile = src.profile
        profile.update(dtype='float32')
        profile.update(count=1)
        with rasterio.open('output', 'w', **profile) as dst:
            for _, window in src.block_windows(1):
                red = src.read(1, window=window).astype('float32')
                nir = src.read(2, window=window).astype('float32')
                ndvi = (np.where((nir + red) == 0., 0,
                                 (nir - red) / (nir + red))).astype('float32')
                dst.write(ndvi, 1, window=window)

    result_item = item[:37]+"_NDVI.tif"
    ibm_cos.put_object(Bucket=bucket, Key=result_item,
                       Body=open('output', 'rb').read())

    return result_item


def get_tile_id(metadata_key, bucket, ibm_cos):
    import pickle
    print(metadata_key, bucket)

    res = ibm_cos.get_object(Bucket=bucket, Key=metadata_key)
    meta = pickle.loads(res['Body'].read())
    return meta['filename']


def group_tiles(items, geotiff_items):
    import pickle
    import re
    from collections import defaultdict

    tiles = defaultdict(list)

    items = eval(items)
    geotiff_items = eval(geotiff_items)

    for item in items:
        tile = item[39:44]
        date = item[11:19]

        regex = r".*" + re.escape(date) + r".*" + re.escape(tile) + \
            r".*" + re.escape('NDVI') + r".*\." + re.escape('tif')

        filtered = [it for it in geotiff_items if re.search(regex, it)]
        print('filtered: {}'.format(filtered))
        if len(filtered) == 1:
            item = filtered.pop()
            tiles[(tile, date[:-2])].append(item)

    grouped_tiles = []
    for key in tiles.keys():
        tile, date = key
        grouped_tiles.append({
            'tile': tile,
            'month': date,
            'items': tiles[key]
        })

    return grouped_tiles


def avg_map_ndvi(tile, month, items, bucket, ibm_cos):
    import os
    import numpy as np
    import rasterio

    result_item = "AVERAGE-NDVI-" + tile + "-" + month + "_MONTH.tif"

    if not items:
        return

    # Get profile from first object
    obj = items[0]
    with rasterio.open(ibm_cos.get_object(Bucket=bucket, Key=obj)['Body']) as src:
        profile = src.profile
        profile.update(compress='DEFLATE')
        # profile.update(zlevel=9)
        # profile.update(predictor=2)
        # profile.update(DISCARD_LSB=2)
        valid_count = np.zeros((src.height, src.width), dtype='uint8')

    # Write averaged value
    with rasterio.open('output', 'w+', **profile) as dst:
        for obj in items:
            cos_object = ibm_cos.get_object(Bucket=bucket, Key=obj)
            with rasterio.open(cos_object['Body']) as src:
                for ji, window in src.block_windows(1):
                    valid_count[window.row_off:window.row_off+window.height,
                                window.col_off:window.col_off+window.width] += src.read_masks(1, window=window).astype('bool')
                    blockfile = 'geotiff-block-' + \
                        str(ji[0])+'-'+str(ji[1]) + ".npy"
                    ndvi = np.load(blockfile) if os.path.exists(blockfile) else np.zeros(
                        (window.height, window.width), dtype='float32')
                    ndvi += src.read(1, window=window)
                    np.save(blockfile, ndvi)
                    #dst.write((src.read(1, window=window) + dst.read(1, window=window)).astype('float32'), 1, window=window)

        valid_count = np.where(valid_count == 0, 1, valid_count)

        for ji, window in dst.block_windows(1):
            block_count = valid_count[window.row_off:window.row_off+window.height,
                                      window.col_off:window.col_off+window.width]
            blockfile = 'geotiff-block-'+str(ji[0])+'-'+str(ji[1]) + ".npy"
            ndvi = np.load(blockfile)
            #ndvi = dst.read(1, window=window)
            dst.write(ndvi/block_count, 1, window=window)
    ibm_cos.put_object(Bucket=bucket, Key=result_item,
                       Body=open('output', 'rb'))
    return result_item


def split_tiles(items, geotiff_items, splits):
    import re
    import pickle

    print('items: {} -- type: {}'.format(items, type(items)))
    print('geotiff_items: {} -- type: {}'.format(geotiff_items, type(items)))
    print('splits: {} -- type: {}'.format(splits, type(items)))
    items = eval(items)
    geotiff_items = eval(geotiff_items)
    splits = int(splits)

    splits_iterdata = []

    for item in items:
        tile = item[39:44]
        date = item[11:17]

        pattern = r".*" + tile + r".*" + date + r".*"
        filtered = [t for t in geotiff_items if re.search(pattern, t)]

        for geotiff_item in filtered:
            for i in range(splits):
                for j in range(splits):
                    splits_iterdata.append({'item': geotiff_item,
                                            'tile': tile,
                                            'date': date,
                                            'block_x': j, 'block_y': i,
                                            'splits': splits})

    return(splits_iterdata)


def avg_shape_ndvi(item, tile, date, block_x, block_y, splits, bucket, ibm_cos):
    # Imports needed
    import numpy as np
    import fiona
    import rasterio.mask
    from rasterio import features
    import math
    from shapely.geometry import shape, box
    from rasterio.windows import Window
    from functools import partial

    MARGIN = 512  # MARGIN defines a larger window to work with in order to avoid cutting geometries
    result_key = "tmp/" + \
        item[:item.rfind('_')] + "_" + str(block_x) + \
        "_" + str(block_y) + "_SHAPE.tif"

    # Download shapefile
    shapefile = ibm_cos.get_object(Bucket=bucket, Key='shapefile.zip')['Body']
    with open('shape.zip', 'wb') as shapf:
        for chunk in iter(partial(shapefile.read, 450 * 1024 * 1024), ''):
            if not chunk:
                break
            shapf.write(chunk)

    chunk = ibm_cos.get_object(Bucket=bucket, Key=item)['Body']
    with rasterio.open(chunk) as src:
        step_w = src.width/splits
        step_h = src.height/splits

        profile = src.profile
        profile.update(width=step_w)
        profile.update(height=step_h)

        transform = src.transform

        # Get writing window (curr_window) and working window (big_window)
        # with their bounds and affine transforms
        curr_window = Window(step_w*block_y, step_h*block_x, step_w, step_h)
        left = int(max(0, step_w*block_y - MARGIN))
        width = int(min(step_w+2*MARGIN, src.width - left))
        top = int(max(0, step_h*block_x - MARGIN))
        height = int(min(step_h+2*MARGIN, src.height - top))
        big_window = Window(left, top, width, height)
        bounds = rasterio.windows.bounds(curr_window, transform)
        big_bounds = rasterio.windows.bounds(big_window, transform)
        big_left, big_bottom, big_right, big_top = big_bounds
        big_transform = rasterio.windows.transform(big_window, transform)

        content = src.read(1, window=big_window)
        result = np.zeros((height, width), dtype='float32')
        result_file = "output-"+str(block_x)+"-"+str(block_y)
        with rasterio.open(result_file, "w", **profile) as dest:
            with fiona.open('zip://shape.zip') as shape_src:
                for feature in shape_src.filter(bbox=bounds):
                    geom = shape(feature['geometry'])
                    # Get bounding window of geometry
                    left, bottom, right, top = geom.bounds
                    window = src.window(max(left, big_left), max(
                        bottom, big_bottom), min(right, big_right), min(top, big_top))
                    window_floored = window.round_offsets(
                        op='floor', pixel_precision=3)
                    w = math.ceil(window.width + window.col_off -
                                  window_floored.col_off)
                    h = math.ceil(window.height +
                                  window.row_off - window_floored.row_off)
                    window = Window(window_floored.col_off,
                                    window_floored.row_off, w, h)
                    win_transform = rasterio.windows.transform(
                        window, transform)
                    # Convert shape to raster matrix
                    image = features.rasterize([geom],
                                               out_shape=(h, w),
                                               transform=win_transform,
                                               fill=0,
                                               default_value=1).astype('bool')

                    # Get ndvi values from rasterized image and write them to result matrix
                    if np.any(image):
                        start_w = window.col_off - big_window.col_off
                        end_w = start_w + window.width
                        start_h = window.row_off - big_window.row_off
                        end_h = start_h + window.height
                        crop = content[start_h:end_h, start_w:end_w]
                        result[start_h:end_h,
                               start_w:end_w][image] = crop[image].mean()

            # Write results to output file
            start_w = int(curr_window.col_off - big_window.col_off)
            start_h = int(curr_window.row_off - big_window.row_off)
            limit_w = int(curr_window.width + start_w)
            limit_h = int(curr_window.height + start_h)
            dest.write(result[start_h:limit_h, start_w:limit_w], 1)

    ibm_cos.put_object(Bucket=bucket, Key=result_key,
                       Body=open(result_file, 'rb'))
    return result_key


def gather_blocks(item, splits, bucket, ibm_cos):
    import rasterio
    from rasterio.windows import Window

    chunks = [{'key': 'tmp/{}_{}_{}_SHAPE.tif'.format(item[:item.rfind('_')], x, y),
               'x': x,
               'y': y} for x in range(splits) for y in range(splits)]

    # Open first object to obtain profile metadata
    obj = list(chunks)[0]['key']
    print(obj)
    chunk = ibm_cos.get_object(Bucket=bucket, Key=obj)['Body']
    with rasterio.open(chunk) as src:
        profile = src.profile
        profile.update(width=src.width*splits)
        profile.update(height=src.height*splits)
        stepW = src.width
        stepH = src.height

    result_key = item[:item.rfind('_')] + "_SHAPE.tif"

    # Iterate each object and print its block into the destination file
    with rasterio.open("output", "w", **profile) as dest:
        for chunk in chunks:
            with rasterio.open(ibm_cos.get_object(Bucket=bucket, Key=chunk['key'])['Body']) as src:
                curr_window = Window(
                    stepW*chunk['x'], stepH*chunk['y'], stepW, stepH)
                content = src.read(1)
                dest.write(content, 1, window=curr_window)

    ibm_cos.put_object(Bucket=bucket, Key=result_key,
                       Body=open("output", 'rb'))
    return result_key


def clean_tmp(bucket, ibm_cos):
    while True:
        tmp_objects = ibm_cos.list_objects(Bucket=bucket, Prefix="tmp/")

        delete_keys = [obj['Key'] for obj in tmp_objects.get('Contents', [])]

        if not delete_keys:
            break

        for key in delete_keys:
            ibm_cos.delete_object(Bucket=bucket, Key=key)

