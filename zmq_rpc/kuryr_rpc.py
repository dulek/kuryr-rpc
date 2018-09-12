import zmq
import time
import sys
import eventlet
import json
import ast
import pudb

from locks import RWLock

# Templates
SYNC = {
    "type":"sync",
    "name": "",
    "phase": 0,
    "data":{}
}

NOTIFY = {
    "type": "notify",
    "name": "",
    "message":""
}

ACK_OK = {
    "type": "ack",
    "message": "ok"
}

INVALID = {
    "type": "error",
    "message": "Invalid message"
}


# No longer used
class Publisher:
    def __init__(self, address="tcp://*:5556"):
        """Simpler Publisher Server Object

        address: [string] url the server is hosted on
        """

        self.context = zmq.Context()
        self.socket  = self.context.socket(zmq.PUB)
        self.socket.bind(address)
        print("Setting up a Publisher at: %s" % (address))

    def publish(self, message):
        if message and type(message) == dict:
            self.socket.send_json(message)
        else:
            raise ValueError("ValueError: Your message must be"
                             "a python dictionary!")

    def close(self):
        self.socket.close()


class Subscriber:
    def __init__(self, address, filter=''):
        self.context = zmq.Context()
        self.socket  = self.context.socket(zmq.SUB)
        self.socket.connect(address)
        self.socket.setsockopt(zmq.SUBSCRIBE, filter)
        print("Subscribing to %s" % address)

    def getMessage(self):
        return self.socket.recv_json()

    def close(self):
        self.socket.close()


class RPCServer:
    def __init__(self, address):
        self.context = zmq.Context()
        self.socket  = self.context.socket(zmq.REP)
        self.socket.bind(address)
        print("Setting up an RPC Server at: %s" % (address))

    def getRequest(self):
        return ast.literal_eval(self.socket.recv_json())

    def reply(self, message):
        self.socket.send_json(message)

    def close(self):
        self.socket.close()

class RPCClient:
    '''A Lazy client that will not block in the event
    of a server failure.

    Source
    zmq manual: http://zguide.zeromq.org/py:lpclient
    '''

    def __init__(self, address, max_timeout=2500):
        self.timeout = max_timeout
        self.max_retries = 5
        self.server_address = address

        self.context = zmq.Context()
        self.socket  = self.context.socket(zmq.REQ)
        self.socket.connect(address)
        print("Establishing a connection to: %s" % address)

        self.poll = zmq.Poller()
        self.poll.register(self.socket, zmq.POLLIN)

    def send(self, message):
        """Lazy Poller

        Source: Lazy Pirate Client
        http://zguide.zeromq.org/py:lpclient
        """

        if message and type(message) == dict:
            sequence = 0
            retries_left = self.max_retries
            while retries_left:
                sequence += 1
                request = str(message)
                print("I: Sending (%s)" % request)
                self.socket.send_json(request)
                expect_reply = True
                while expect_reply:
                    socks = dict(self.poll.poll(self.timeout))
                    if socks.get(self.socket) == zmq.POLLIN:
                        reply = self.socket.recv_json()
                        if not reply:
                            break
                        else:
                            retries_left = retries_left
                            expect_reply = False
                            return reply

                    else:
                        print("No response from server, retrying...")
                        # Socket is confused. Close and remove it.
                        self.socket.setsockopt(zmq.LINGER, 0)
                        self.socket.close()
                        self.poll.unregister(self.socket)
                        retries_left -= 1
                        if retries_left == 0:
                            print("E: Server seems to be offline, abandoning")
                            break
                        print("I: Reconnecting and resending (%s)" % request)
                        # Create new connection
                        client = self.context.socket(zmq.REQ)
                        client.connect(self.server_address)
                        self.poll.register(client, zmq.POLLIN)
                        client.send(request)
        else:
            raise ValueError("ValueError: Your message must be"
                             "a python dictionary!")

    def close(self):
        self.socket.close()

class Switch:
    """A thread safe boolean switch
    """
    def __init__(self):
        self.value = False
        self.lock = RWLock()

    def get(self):
        self.lock.reader_acquire()
        value = self.value
        self.lock.reader_release()
        return value

    def on(self):
        self.lock.writer_acquire()
        self.value = True
        self.lock.writer_release()

    def off(self):
        self.lock.writer_acquire()
        self.value = False
        self.lock.writer_release()


class MasterServer():
    def __init__(self, members, name, server_address='tcp://*:5556', pubsub_address='tcp://*:5557'):
        """RPC server that runs on the master node.

        Note: Currently, the server is not threaded. A high volume of
                traffic is not expected, so its probably not necessary.

        Arguments:
        members: [list] the namespace/name of running servers in the cluster
        name: [string] the namespace/name of the node this is running on
        server_address: [string] the web address the main server runs on
        pubsub_address: [string] the web address the pub_sub runs on
        """

        self.server = RPCServer(server_address)
        self.publisher = Publisher(pubsub_address)
        self.message_queue = eventlet.Queue()
        self.running = Switch()
        self.running.on()
        self.name = name
        self.phase = 0
        self.num_repoted = 0
        self.busy = False

        # TODO(egarcia): Make this thread safe
        self.members = {}
        for member in members:
            self.members[member] = {
                'phase': 0,
                'data': {}
            }

        # Start the server
        self.__listen()

    # Main server body
    def __listen(self):
        while self.running.get():
            try:
                self.busy = True
                request = self.server.getRequest()
                print(request)
                client_name = request['name']
                if client_name in self.members:
                    if request['type'] == 'notify':
                        print "Received a notification from %s" % client_name
                        if request['message'] == 'membership change' and self.phase == 0:
                            print("A membership change has occured")
                            self.server.reply(ACK_OK)
                            self._initiate_resizing()
                    elif request['type'] == 'sync':
                        if self.phase == 0 or request['phase'] == 0:
                            print("Incorrectly formatted request! Discarding...")
                            self.server.reply(INVALID)
                        if self.phase == 1 and self._phases_correct(request):
                            self.members[client_name]['phase'] = 1
                            self.members[client_name]['data']['current_job'] = request['data']
                            self.server.reply(ACK_OK)
                            if self._phase_complete(1):
                                self._start_phase_2()
                        if self.phase == 2 and self._phases_correct(request):
                            if request['data']['ack'] == 'ok':
                                self.members[client_name]['phase'] = 2
                                self.server.reply(ACK_OK)
                                if self._phase_complete(2):
                                    self._start_phase_3()
                        if self.phase == 3 and self._phases_correct(request):
                            if request['data']['ack'] == 'ok':
                                self.members[client_name]['phase'] = 3
                                self.server.reply(ACK_OK)
                                if self._phase_complete(3):
                                    self._finish_resizing()
                else:
                    self.server.reply(INVALID)
                self.busy = False
            except Exception as e:
                print(e)

    # Hash Synchronization Steps
    def _initiate_resizing(self):
        #TODO(egarcia):lock job queue

        #Prune out dead members from self.members
        alive = self._get_healthy_members()
        for member in self.members:
            if member not in alive:
                self.members.pop(member, None)

        #Begin resizing protocol
        self.phase = 1
        self.members[self.name]['data']['current_job'] = self._get_current_job()
        self.members[self.name]['phase'] = 1
        msg = SYNC
        msg['phase'] = 1
        self.publisher.publish(msg)

    def _start_phase_2(self):
        self.phase = 2
        latest_job = self._get_latest_job()
        msg = SYNC
        msg['phase'] = 2
        msg['data']['sync to'] = latest_job
        msg['data']['members'] = self._get_healthy_members()
        self.publisher.publish(msg)
        self._synchronize(latest_job)

    def _start_phase_3(self):
        self.phase = 3
        msg = SYNC
        msg['phase'] = 3
        self.publisher.publish(msg)
        self._resume()

    def _finish_resizing(self):

        # Mark all nodes as returning to state 0 (running normally)
        for member in self.members.keys():
            self.members[member] = {
                'phase': 0,
                'data': {}
            }

    # Helper Functions
    def _phases_correct(self, request):
        return self.phase == request['phase'] and self.members[request['name']]['phase'] != self.phase

    def _phase_complete(self, phase):
        for key in self.members.keys():
            if self.members[key]['phase'] != phase:
                return False
        return True

    def _synchronize(self, latest_job):
        """Synchronize your state with the cluster
        
        Execute the jobs in your queue up until latest_job, then
        update your hash ring
        """
        # TODO(egarcia): Thread this process
        self.members[self.name]['phase'] = 2

    def _resume(self):
        # TODO(egarcia): Resume processing jobs

        # Mark yourself done
        self.members[self.name]['phase'] = 3

    def _get_latest_job(self):
        """Finds the state of the fastest server and syncs the cluster to that
        """
        pass

    def _get_current_job(self):
        """Gets the next job the system is going to process
        """
        pass

    def _get_healthy_members(self):
        """Use Kubernetes API to get running kuryr-controller pod names
        """
        pass

    # Client Functions
    def membershit_change(self, new_clients):
        if self.phase == 0:
            print("Initiating a Ring Resizing")
        else:
            print("A node has died during resizing")
            #Check phase change

    def shutdown(self):
        print("Attempting Graceful Shutdown...")
        while self.phase != 0 or self.busy:
            time.sleep(0.2)
        self.running.off()
        self.server.close()
        self.publisher.close()
        #TODO(egarcia): implement server termination


class Client():
    def __init__(self, name, server_address='tcp://*:5556', pubsub_address='tcp://*:5557'):
        self.name = name
        self.client = RPCClient(server_address)
        self.subscriber = Subscriber(pubsub_address)
        self.phase = 0

    def notify_membership_change(self):
        pass

    def watch_master(self):
        pass
