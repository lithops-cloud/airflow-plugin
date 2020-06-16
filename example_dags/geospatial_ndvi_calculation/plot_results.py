from matplotlib import pyplot as plt
from ibm_botocore.client import Config, ClientError
import rasterio
import random
import ibm_boto3

ENDPOINT_URL='<ENDPOINT_URL>'
IBM_API_KEY_ID='<IBM_API_KEY_ID>'
IBM_SERVICE_INSTANCE_ID='<IBM_SERVICE_INSTANCE_ID>'
BUCKET='<BUCKET>'

def plot_results(results):
    """
    Plot an array of COS from IBM Cloud
    """
    size = len(results)
    fig, axs = plt.subplots(len(results), figsize=(20, 30))

    cos = ibm_boto3.client("s3",
                           config=Config(signature_version="oauth"),
                           endpoint_url=ENDPOINT_URL,
                           ibm_api_key_id=IBM_API_KEY_ID,
                           ibm_service_instance_id=IBM_SERVICE_INSTANCE_ID)
    i = 1
    for item in results:
        with rasterio.open(cos.get_object(Bucket=BUCKET, Key=item)['Body']) as src:
            arr = src.read(1, out_shape=(src.height//10, src.width//10))
            plt.subplot(1 + (size-1)/2, 2, i)
            plt.gca().set_title(item)
            plt.imshow(arr)
            plt.colorbar(shrink=0.5)
            i += 1

    plt.savefig('result.png', format='png')


if __name__ == '__main__':
    plot_results(['AVERAGE-NDVI-30SXG-201911_MONTH.tif'])
