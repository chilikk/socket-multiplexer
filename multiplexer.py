#!/usr/bin/env python3

import logging
import select
import socket
import time
import threading

# address and port the publisher is listening on
PUBLISHER = ("127.0.0.1", 2000)
# address and port to listen to incoming connections from listeners
# it is a security risk to set LISTEN address to something else than 127.0.0.1!
LISTEN = ("127.0.0.1", 2001)

LOG_LEVEL = logging.INFO

BUF_SIZE = 2048

class Reader(threading.Thread):
    def __init__(self):
        name = "Reader({},{})".format(PUBLISHER[0], PUBLISHER[1])
        self.logger = logging.getLogger(name)
        super(Reader, self).__init__(name=name)
        while True:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(10)
                self.logger.info("Connecting to %s:%d...",
                                 PUBLISHER[0], PUBLISHER[1])
                self.sock.connect(PUBLISHER)
                break
            except ConnectionRefusedError:
                self.logger.debug("Connection refused")
                time.sleep(10)
            except socket.timeout:
                self.logger.debug("Connection timed out")
        self.logger.info("Connection successful")
        self.handlers = []
        self.acceptor = Acceptor()
        self.handlers = self.acceptor.handlers
        self.shutdown_requested = False
        self.acceptor.start()

    def run(self):
        try:
            while True:
                try:
                    select.select([self.sock], [], [])
                    if self.shutdown_requested:
                        self.logger.info("Reader is shutting down")
                        break
                    data = self.sock.recv(BUF_SIZE)
                    if not data:
                        self.logger.info("Source disconnected. Shutting down")
                        break
                except ConnectionResetError:
                    self.logger.error("Source disconnected abruptly. " \
                                      "Shutting down")
                    break
                for handler in self.handlers:
                    handler.enqueue(data)
        finally:
            self.sock.close()
            self.acceptor.shutdown()

    def shutdown(self):
        self.shutdown_requested = True
        self.sock.shutdown(socket.SHUT_RDWR)

class Acceptor(threading.Thread):
    def __init__(self, **kwargs):
        kwargs["name"] = "Acceptor({},{})".format(LISTEN[0], LISTEN[1])
        self.logger = logging.getLogger(kwargs["name"])
        super(Acceptor, self).__init__(**kwargs)
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.lsock.bind(LISTEN)
        self.lsock.listen()
        self.shutdown_requested = False
        self.handlers = []
        self.logger.info("Listening to incoming connections on %s:%d...",
                         LISTEN[0], LISTEN[1])

    def run(self):
        try:
            while True:
                try:
                    accepted, info = self.lsock.accept()
                except OSError:
                    if self.shutdown_requested:
                        break
                    else:
                        self.logger.error("Error when accepting connection")
                        break
                handler = Handler(self, accepted, info)
                self.handlers.append(handler)
                handler.start()
        finally:
            self.cleanup()

    def deregister_handler(self, handler):
        self.handlers.remove(handler)

    def shutdown(self):
        self.shutdown_requested = True
        self.lsock.shutdown(socket.SHUT_RDWR)

    def cleanup(self):
        self.logger.info("Stopping listening to incoming connections")
        self.lsock.close()
        for handler in self.handlers:
            handler.shutdown()

class Handler(threading.Thread):
    def __init__(self, registry, accepted, info, **kwargs):
        kwargs["name"] = "Handler({},{})".format(info[0], info[1])
        super(Handler, self).__init__(**kwargs)
        self.logger = logging.getLogger(kwargs["name"])
        self.logger.info("New connection from %s:%d", info[0], info[1])
        self.sock = accepted
        self.registry = registry
        self.ssock, self.rsock = socket.socketpair()
        self.shutdown_requested = False

    def run(self):
        try:
            while True:
                ready, _, _ = select.select([self.sock, self.rsock], [], [])
                if self.shutdown_requested:
                    break
                if self.sock in ready:
                    try:
                        ## discard any incoming data on the socket
                        if not self.sock.recv(BUF_SIZE):
                            self.logger.info("Disconnected")
                            break
                    except ConnectionResetError:
                        self.logger.info("Abruptly disconnected")
                        break
                if self.rsock in ready:
                    data = self.rsock.recv(BUF_SIZE)
                    if not data:
                        break
                    try:
                        self.sock.sendall(data)
                    except ConnectionResetError:
                        self.logger.info("Abruptly disconnected")
                        break
        finally:
            self.cleanup()

    def enqueue(self, data):
        try:
            self.ssock.sendall(data)
        except ConnectionResetError:
            self.logger.info("Could not enqueue data. Shutting down")
            self.shutdown()

    def shutdown(self):
        self.logger.info("Handler shutdown requested")
        self.shutdown_requested = True
        self.ssock.shutdown(socket.SHUT_RDWR)

    def cleanup(self):
        self.logger.info("Handler stopping")
        self.registry.deregister_handler(self)
        self.ssock.close()
        self.rsock.close()
        self.sock.close()

if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)
    while True:
        try:
            READER = Reader()
        except KeyboardInterrupt:
            break
        READER.start()
        try:
            READER.join()
        except KeyboardInterrupt:
            READER.shutdown()
            break
