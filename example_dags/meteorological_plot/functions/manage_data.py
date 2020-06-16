import requests
import gzip
import io
import json

def get_dataset(data_url, bucket, ibm_cos):
    res = requests.get(data_url)
    by = res.content

    with gzip.GzipFile(fileobj=io.BytesIO(by), mode='rb') as gzipf:
        x = gzipf.read()
    
    ibm_cos.put_object(Bucket=bucket, Key='weather_data', Body=x)


def parse_data(obj, countries, bucket, id, ibm_cos):
    s = obj.data_stream.read().decode("utf-8")
    data = [json.loads(d) for d in s.splitlines()]

    samples = dict()
    
    for sample in data:
        if sample['city']['country'] in countries:
            if sample['city']['country'] not in samples:
                samples[sample['city']['country']] = list()
            samples[sample['city']['country']].append(json.dumps(sample))
    
    for k,v in samples.items():
        ibm_cos.put_object(Bucket=bucket, Key='{}/{}'.format(k, id), Body=json.dumps(v))


def get_plot_data(obj, country, bucket, plot):
    samples = json.loads(obj.data_stream.read())

    res_l = list()
    for sample in samples:
        sample_json = json.loads(sample)
        res = dict()
        res['lon'] = sample_json['city']['coord']['lon']
        res['lat'] = sample_json['city']['coord']['lat']
        if plot == 'temp':
            res['temp'] = sample_json['main']['temp'] - 273
        elif plot == 'humi':
            res['humi'] = sample_json['main']['humidity']
        elif plot == 'press':
            res['press'] = sample_json['main']['pressure']
        else:
            raise Exception
        res_l.append(res)
    return res_l