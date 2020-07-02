import time
import socket
import logging
import threading

from threading import Thread

import Messages

from IOTWrapper import init_iot, subscribe, publish

logger = logging.getLogger(__file__)


class CloudHandler(Thread):

    def __init__(self):
        Thread.__init__(self)

        self.daemon = True
        self.shutdown = False

    def panic(self):
        self.shutdown = True

    def run(self):
        init_iot()

        def handle_callback(client, userdata, message):
            logger.debug('cloud handler received message')
            # logger.debug(message.payload)
            msg = Messages.AMessage.deserialize(message.payload)
            if msg:
                msg.handle(path='/tmp/')
            else:
                logger.error('unparsable message')

        subscribe('dev/test01', handle_callback)
        subscribe('dev/data/request', handle_callback)

        while not self.shutdown:
            time.sleep(1)

        shut_iot()


class LocalHandler(Thread):

    def __init__(self, host='127.0.0.1', port=0xBEEF):
        Thread.__init__(self)

        self.sock = None
        self.daemon = True
        self.shutdown = False
        self.thread_id_count = 0
        self.threads = []

        if host and port:
            self._connect(host, port)

    def _connect(self, host, port):
        if not host or not port:
            logger.error('could not open port, invalid arguments')
            return

        if not self.sock:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((host, port))
            self.sock.listen(1)
            logger.debug('brought up server on {}:{}'.format(host, port))

    def panic(self):
        if self.sock:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.shutdown = True

        for t in self.threads:
            t.join()

    def prune(self):
        prunable = [t for t in self.threads if not t.is_alive]
        if prunable:
            logger.info('pruning threads {}'.format(prunable))
            [p.join() for p in prunable] 

    def run(self):
        while True:
            if self.shutdown:
                return

            if not self.sock:
                logger.error('no socket, exiting!')
                break

            try:
                conn, addr = self.sock.accept()
                logger.info('made connection to {}'.format(addr))
            except BaseException as e:
                logger.error('failed to accept connection: {}'.format(e))

            this_id = self.thread_id_count
            self.thread_id_count += 1
            new_thread = Thread(target=self.worker, args=(this_id, conn, addr))
            new_thread.daemon = True
            self.threads.append(new_thread)
            new_thread.start()

            self.prune()


    # handles TCP socket messages between model and local server
    def worker(self, thread, conn, addr):
        logger.info('spawning thread:{} to handle {}'.format(thread, addr))
        count = 0
        while True:
            if self.shutdown:
                return

            msg_string = conn.recv(4096)
            logger.debug('thread:{} received msg num {}'.format(thread, count))
            count += 1

            if not msg_string:
                logger.info('closing connection to {}'.format(addr))
                conn.shutdown(socket.SHUT_RDWR)
                break

            msg = Messages.AMessage.deserialize(msg_string)
            if msg:
                msg.handle(resp_sock=conn)
            else:
                logger.error('unparsable message')

        logger.info('shutting thread {}'.format(thread))


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    lh = LocalHandler()
    lh.start()

    ch = CloudHandler()
    ch.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 0xBEEF))

    def new_p():
        return Messages.ProbabilityMessage([0.1, 0.3, 0.4, 0.1])

    p = new_p()

