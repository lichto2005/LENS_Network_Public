import os
import json
import time
import uuid
import yaml
import logging
import datetime
import requests

import IOTWrapper

logger = logging.getLogger(__file__)


# TODO: this is currently a dummy...
def fetch_metadata():
    return {'device_id': 'none', 'location': 'none', 'datetime': datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}


class AMessage(object):

    def __init__(self, *args, **kwargs):
        self.msg_dict = {}

    def __repr__(self):
        return json.dumps(self.msg_dict, sort_keys=True, indent=4, separators=(',', ': '))

    def serialize(self):
        return json.dumps(self.msg_dict)

    @staticmethod
    def deserialize(json_string):    
        sub_classes = [
                ProbabilityMessage,
                DetectMessage,
                MetadataRequest,
                MetadataResponse,
                FileRequest,
                FileResponse,
                QueryRequest,
                QueryResponse,
                DetectData,
                SendPush
        ]

        for clz in sub_classes:
            try:
                msg = clz._deserialize(json_string)
                # logger.debug('parsed message as {}'.format(clz.__name__))
                return msg
            except BaseException as e:
                # pass
                logger.debug('e: {}'.format(e))
                # logger.debug('failed trying to parse as {}'.format(clz.__name__))

        return None

    @staticmethod
    def _deserialize(json_string):
        raise RuntimeError('_deserialize not implemented')

    def handle(self, *args, **kwargs):
        logger.error('handle not implemented')


class ModelResponse(AMessage):

    def __init__(self, uid):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'model-response'
        self.msg_dict['uid'] = uid

    @staticmethod
    def _deserialize(json_string):
        msg = yaml.safe_load(json_string)
        if msg.get('message', None) == 'model-response':
            ret = ModelResponse(None)
            ret.msg_dict = msg
            return ret

        raise RuntimeError('bad parse')

    def handle(self, *args, **kwargs):
        buf = kwargs.get("buf", None)
        save_fn = kwargs.get("func", None)
        path = kwargs.get("path", None)
        uid = self.msg_dict.get('uid', None)
        if buf and save_fn and uid and path:
            logger.debug('saving clip {}'.format(uid))
            save_fn('{}/{}'.format(path, uid), buf)


class ProbabilityMessage(AMessage):
    
    idx_to_crime_dict = {
        0: 'theft',
        1: 'assault',
        2: 'shooting',
        3: 'no action'
    }

    last_seen = None
    last_type = None

    def __init__(self, predictions):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'predictions'
        predictions = [str(x) for x in predictions] if predictions else None
        self.msg_dict['probabilities'] = predictions
        self.msg_dict['time'] = str(time.time())

    @staticmethod
    def _deserialize(json_string):
        msg = yaml.safe_load(json_string)
        if msg.get('message', None) == 'predictions':
            ret = ProbabilityMessage(None)
            ret.msg_dict = msg
            return ret

        raise RuntimeError('bad parse')

    def handle(self, *args, **kwargs):
        # figure out the highest prediction
        preds = [float(x) for x in self.msg_dict['probabilities']]
        logger.info('received predictions: {}'.format(preds))
        max_idx = 0
        max_pred = 0.0
        for i, p in enumerate(preds):
            if p > max_pred:
                max_pred = p
                max_idx = i

        uid = None

        # make sure it is a valid one
        crime_type = self.idx_to_crime_dict.get(max_idx, 'no action')
        crime_time = float(self.msg_dict.get('time', 0))
        if crime_type == 'no action':
            logger.error('either preds are bad or we received a "no action" msg')
        elif not crime_time:
            logger.error('pred message has no time')
        # ignore a prediction if its the same prediction < 1 second apart
        elif (crime_type == ProbabilityMessage.last_type and
                crime_time - ProbabilityMessage.last_seen < 1):
            logger.info('saw this same crime < 1sec ago, ignoring')
            ProbabilityMessage.last_seen = crime_time
        # send a detect
        else:
            logging.info('publishing new crime to IOT')
            ProbabilityMessage.last_seen = crime_time
            ProbabilityMessage.last_type = crime_type
            msg = DetectMessage(crime_time, crime_type, 'Boston', max_pred)
            IOTWrapper.publish('dev/detect', msg.serialize())

            uid = msg.msg_dict['metadata']['uid']

        logger.info('sending response to model')
        conn = kwargs.get('resp_sock', None)
        if conn:
            conn.send(ModelResponse(uid).serialize())


class DetectMessage(AMessage):

    def __init__(self, det_time, action_type, location, prob):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'detect'
        self.msg_dict['detect_type'] = action_type
        self.msg_dict['metadata'] = {
                'uid': str(uuid.uuid4()),
                'timestamp': det_time,
                'detect_type': action_type,
                'location': location,
                'prob': prob,
        }

    @staticmethod
    def _deserialize(json_string):
        msg = yaml.safe_load(json_string)
        if msg.get('message', None) == 'detect':
            ret = DetectMessage(None, None, None, None)
            ret.msg_dict = msg
            return ret

        raise RuntimeError('bad parse')

    def handle(self, *args, **kwargs):
        logger.info('DetectMessage should be handled by server')


class MetadataRequest(AMessage):

    def __init__(self, device_id):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'metadata-request'
        self.msg_dict['device_id'] = device_id


class MetadataResponse(AMessage):

    def __init__(self):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'metadata-response'
        self.msg_dict['metadata'] = fetch_metadata()


class FileRequest(AMessage):

    def __init__(self, filename, url, headers):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'file-request'
        self.msg_dict['filename'] = filename
        self.msg_dict['s3_url'] = url
        self.msg_dict['headers'] = headers
    
    @staticmethod
    def _deserialize(json_string):
        msg = yaml.safe_load(json_string)
        if msg.get('message', None) == 'file-request':
            ret = FileRequest(None, None, None)
            ret.msg_dict = msg
            return ret

        raise RuntimeError('bad parse')

    def handle(self, *args, **kwargs):
        filename = self.msg_dict.get("filename", None)
        url = self.msg_dict.get("s3_url", None)
        headers = self.msg_dict.get("headers", None)
        path = kwargs.get("path", None)
        if not url or not filename or not headers:
            logger.error('bad file request')
            return

        if path:
            filename = os.path.join(path, filename)

        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                files = {'file': (filename, f)}
                resp = requests.post(url, data=headers, files=files)

            try:
                resp.raise_for_status()
                logger.info('uploaded {}'.format(filename))
            except requests.exceptions.HTTPError as e:
                logger.error('POST to S3 failed: {}'.format(e))

            msg = FileResponse(resp.status_code)
        else:
            logger.error('requested file does not exist {}'.format(filename))
            msg = FileResponse(404)
        IOTWrapper.publish('dev/data/response', msg.serialize())


class FileResponse(AMessage):

    def __init__(self, status):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'file-response'
        self.msg_dict['status'] = status


class QueryRequest(AMessage):

    def __init__(self, detect_id, prefix):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'query-request'
        self.msg_dict['detect_id'] = detect_id
        self.msg_dict['optional'] = prefix


class QueryResponse(AMessage):

    def __init__(self, results):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'query-response'
        self.msg_dict['results'] = [r.msg_dict for r in results]


class DetectData(AMessage):

    def __init__(self, detect_id):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'detect-data'
        self.msg_dict['uid'] = detect_id
    
    @staticmethod
    def _deserialize(json_string):
        msg = yaml.safe_load(json_string)
        if msg.get('message', None) == 'detect-data':
            ret = DetectData(None)
            ret.msg_dict = msg
            return ret

        raise RuntimeError('bad parse')
    
    def handle(self, *args, **kwargs):
        logger.info('DetectData should be received by cloud')
        

class SendPush(AMessage):

    def __init__(self, push_type, who):
        AMessage.__init__(self)

        self.msg_dict['message'] = 'send-push'
        self.msg_dict['type'] = push_type
        self.msg_dict['who'] = who


if __name__ == '__main__':

    tests = [
        DetectMessage('theft'),
        MetadataRequest(fetch_metadata()['device_id']),
        MetadataResponse(),
        FileRequest('foo.jpg', 's3.aws.amazon.com/endpoint/url'),
        FileResponse(200),
        QueryRequest('theft-001', None),
        QueryResponse([DetectMessage('theft'), DetectMessage('theft')]),
        DetectData('theft-002'),
        SendPush('sms', 'device-001')
    ]

    print 'Executing Basic Unit Test Suite for %s\n' % __file__

    for msg in tests:
        print '----- %s -----' % type(msg).__name__
        print msg
        print ''

