import json
import socket
import logging
import Queue

from threading import Thread

from Messages import ProbabilityMessage, ModelResponse

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SendUtility(Thread):

    def __init__(self, save_folder, save_fn, host='127.0.0.1', port=0xBEEF):
        Thread.__init__(self)

        self.host = host
        self.port = port

        self.daemon = True

        self.queue = Queue.Queue()
        self.save_folder = save_folder
        self.save_fn = save_fn

    def add_to_queue(self, frame_buf, predictions):
        self.queue.put((frame_buf, predictions))

    def send_predictions(self, predictions):
        msg = ProbabilityMessage(predictions)

        for x in xrange(5):
            try:
                self.sock.send(msg.serialize())
                return
            except BaseException:
                raise
                logger.debug('failed to send on try {}'.format(x))

        raise RuntimeError('failed to send predictions')

    def receive_response(self, buf):
        msg_string = self.sock.recv(4096)
        logger.debug('received response: {}'.format(msg_string))

        try:
            msg = ModelResponse._deserialize(msg_string)
        except BaseException as e:
            logger.error('bad response: {}'.format(e))

        if msg:
            msg.handle(func=save_fn, buf=buf, path=self.save_folder)
        else:
            logger.error('no msg')


    def run(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

        while True:
            if not self.queue.empty():
                frame_buf, preds = self.queue.get()
                self.send_predictions(preds)
                self.receive_response(frame_buf)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    def save_fn(path, buf):
        logger.info('saving buffer to {}'.format(path))
    
    s = SendUtility('./demo', save_fn)
    s.start()

    s.add_to_queue([1, 2, 3], [0.1, 0.4, 0.2, 0.3])

