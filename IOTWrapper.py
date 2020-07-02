import os
import time
import logging

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient as Client

logging.getLogger('AWSIoTPythonSDK.core').setLevel(logging.INFO)
logger = logging.getLogger(__name__)

iot_client = None


def fetch_creds(folder=None):
    if folder and os.path.isdir(folder):
        logger.debug('loading IOT creds from {}'.format(folder))
        files = os.listdir(folder)
        ca = next(iter([x for x in files if x.endswith('.pem')]), None)
        key = next(iter([x for x in files if x.endswith('-private.pem.key')]), None)
        cert = next(iter([x for x in files if x.endswith('.crt')]), None)
        return (ca, key, cert)
    else:
        logger.debug('loading IOT creds from environment')
        os.environ.get('IOT_CA', None)
        os.environ.get('IOT_KEY', None)
        os.environ.get('IOT_CERT', None)
        return (ca, key, cert)


def init_iot():
    global iot_client
    if iot_client:
        return

    logger.info('starting IOT client')
    iot_client = Client('CLIENT_NAME')
    
    # TODO: don't hard code this
    iot_client.configureEndpoint('ENDPOINT', 8883)
    iot_client.configureCredentials("Root CA", "private.pem.key", "certificate.pem.crt")

    ca, key, cert = fetch_creds('.')
    logger.info('CA={}'.format(ca))
    logger.info('KEY={}'.format(key))
    logger.info('CERT={}'.format(cert))
    #iot_client.configureCredentials(ca, key, cert)

    iot_client.configureOfflinePublishQueueing(-1)
    iot_client.configureDrainingFrequency(2)
    iot_client.configureConnectDisconnectTimeout(10)
    iot_client.configureMQTTOperationTimeout(5)

    iot_client.connect()


def shut_iot():
    global iot_client
    if iot_client:
        logger.info('closing IOT client')
        iot_client.disconnect()
        iot_client = None


def subscribe(topic, callback):
    global iot_client
    logger.info('subscribing to {}...'.format(topic))
    if not iot_client:
        init_iot()

    iot_client.subscribe(topic, 1, callback)
    time.sleep(2)
    logger.info('subscribed')

def publish(topic, json_string):
    global iot_client
    if not iot_client:
        init_iot()

    try:
        iot_client.publish(topic, json_string, 0)
        logger.info('published to {}'.format(topic))
    except BaseException as e:
        logger.error('error while publishing:')
        logger.error(e)

