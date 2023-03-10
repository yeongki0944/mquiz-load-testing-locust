import random

import requests
from locust import User, TaskSet, events, task, HttpUser
import json
import time
from threading import Thread
import websocket
import logging
from urllib.parse import urlparse

# =========================================================================
# =========================== Stomp Frame  ================================
# =========================================================================

Byte = {
    'LF': '\x0A',
    'NULL': '\x00'
}


class Frame:

    def __init__(self, command, headers, body):
        self.command = command
        self.headers = headers
        self.body = '' if body is None else body

    def __str__(self):
        lines = [self.command]
        skipContentLength = 'content-length' in self.headers
        if skipContentLength:
            del self.headers['content-length']

        for name in self.headers:
            value = self.headers[name]
            lines.append("" + name + ":" + value)

        if self.body is not None and not skipContentLength:
            lines.append("content-length:" + str(len(self.body)))

        lines.append(Byte['LF'] + self.body)
        return Byte['LF'].join(lines)

    @staticmethod
    def unmarshall_single(data):
        lines = data.split(Byte['LF'])

        command = lines[0].strip()
        headers = {}

        # get all headers
        i = 1
        while lines[i] != '':
            # get key, value from raw header
            (key, value) = lines[i].split(':')
            headers[key] = value
            i += 1

        # set body to None if there is no body
        body = None if lines[i + 1] == Byte['NULL'] else lines[i + 1][:-1]

        return Frame(command, headers, body)

    @staticmethod
    def marshall(command, headers, body):
        return str(Frame(command, headers, body)) + Byte['NULL']


# =========================================================================
# =========================== Stomp Client ================================
# =========================================================================

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
VERSIONS = '1.0,1.1'


class Client:
    _locust_environment = None

    def __init__(self, url):

        self.url = url
        self.ws = websocket.WebSocketApp(self.url)
        self.ws.on_open = self._on_open
        self.ws.on_message = self._on_message
        self.ws.on_error = self._on_error
        self.ws.on_close = self._on_close

        self.opened = False

        self.connected = False

        self.counter = 0
        self.subscriptions = {}

        self._connectCallback = None
        self.errorCallback = None

    def _connect(self, timeout=0):
        thread = Thread(target=self.ws.run_forever)
        thread.daemon = True
        thread.start()
        total_ms = 0
        while self.opened is False:
            time.sleep(.25)
            total_ms += 250
            if 0 < timeout < total_ms:
                raise TimeoutError(f"Connection to {self.url} timed out")

    def _on_open(self, ws_app, *args):
        self.opened = True

    def _on_close(self, ws_app, *args):
        self.connected = False
        logging.debug("Whoops! Lost connection to " + self.ws.url)
        self._clean_up()

    def _on_error(self, ws_app, error, *args):
        logging.debug(error)

    def _on_message(self, ws_app, message, *args):
        print("\n<<< " + str(message))
        logging.debug("\n<<< " + str(message))
        frame = Frame.unmarshall_single(message)
        _results = []
        if frame.command == "CONNECTED":
            self.connected = True
            logging.debug("connected to server " + self.url)
            if self._connectCallback is not None:
                _results.append(self._connectCallback(frame))
        elif frame.command == "MESSAGE":
            print(str(frame))

            subscription = frame.headers['subscription']

            if subscription in self.subscriptions:
                onreceive = self.subscriptions[subscription]
                messageID = frame.headers['message-id']

                def ack(headers):
                    if headers is None:
                        headers = {}
                    return self.ack(messageID, subscription, headers)

                def nack(headers):
                    if headers is None:
                        headers = {}
                    return self.nack(messageID, subscription, headers)

                frame.ack = ack
                frame.nack = nack

                _results.append(onreceive(frame))
            else:
                info = "Unhandled received MESSAGE: " + str(frame)
                logging.debug(info)
                _results.append(info)
        elif frame.command == 'RECEIPT':
            print("RECEIPT")
            print(str(frame))
            pass
        elif frame.command == 'ERROR':
            if self.errorCallback is not None:
                _results.append(self.errorCallback(frame))
        else:
            info = "Unhandled received MESSAGE: " + frame.command
            logging.debug(info)
            _results.append(info)

        return _results

    def _transmit(self, command, headers, body=None):
        out = Frame.marshall(command, headers, body)
        logging.debug("\n>>> " + out)
        self.ws.send(out)

    def connect(self, login=None, passcode=None, headers=None, connectCallback=None, errorCallback=None,
                timeout=0):

        start_time = time.time()
        logging.debug("Opening web socket...")
        print(self.url)
        self._connect(timeout)
        headers = headers if headers is not None else {}
        headers['host'] = self.url
        headers['accept-version'] = VERSIONS
        headers['heart-beat'] = '10000,10000'

        if login is not None:
            headers['login'] = login
        if passcode is not None:
            headers['passcode'] = passcode

        self._connectCallback = connectCallback
        self.errorCallback = errorCallback

        self._transmit('CONNECT', headers)

    def disconnect(self, disconnectCallback=None, headers=None):
        if headers is None:
            headers = {}

        self._transmit("DISCONNECT", headers)
        self.ws.on_close = None
        self.ws.close()
        self._clean_up()

        if disconnectCallback is not None:
            disconnectCallback()

    def _clean_up(self):
        self.connected = False

    def send(self, destination, headers=None, body=None):
        if headers is None:
            headers = {}
        if body is None:
            body = ''
        headers['destination'] = destination
        return self._transmit("SEND", headers, body)

    def subscribe(self, destination, callback=None, headers=None):
        print("sub destination : ", destination)
        print("sub destination : ", str(destination))
        if headers is None:
            headers = {}
        if 'id' not in headers:
            headers["id"] = "sub-" + str(self.counter)
            self.counter += 1
        # headers['destination'] = destination
        headers['destination'] = "/pin/123456"
        # self.subscriptions[headers["id"]] = callback
        print(str(headers))
        self._transmit("SUBSCRIBE", headers)

        def unsubscribe():
            self.unsubscribe(headers["id"])

        return headers["id"], unsubscribe

    def unsubscribe(self, id):
        del self.subscriptions[id]
        return self._transmit("UNSUBSCRIBE", {
            "id": id
        })

    def ack(self, message_id, subscription, headers):
        if headers is None:
            headers = {}
        headers["message-id"] = message_id
        headers['subscription'] = subscription
        return self._transmit("ACK", headers)

    def nack(self, message_id, subscription, headers):
        if headers is None:
            headers = {}
        headers["message-id"] = message_id
        headers['subscription'] = subscription
        return self._transmit("NACK", headers)


class StompClient(object):
    def __init__(self, host, port):
        # self.conn = Client("ws://localhost:8080/connect/websocket")
        self.conn = Client("wss://spring.mquiz.site/connect/websocket")

    def __del__(self):
        if self.conn:
            print("disconnect...")
            self.conn.disconnect()

    def start(self):
        start_time = time.time()
        try:
            self.conn.start()
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request_failure.fire(request_type="stomp", name="start", response_time=total_time, exception=e)
        else:
            total_time = int((time.time() - start_time) * 1000)
            events.request_success.fire(request_type="stomp", name="start", response_time=total_time, response_length=0)

    def connect(self):
        start_time = time.time()
        print("Connect")
        try:
            self.conn.connect(login="name",
                              passcode="45C82C421EBA87C8131E220F878E4145",
                              timeout=0)
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            # events.request_failure.fire(request_type="stomp", name="connect", response_time=total_time, exception=e, response_length=0)
        else:
            total_time = int((time.time() - start_time) * 1000)
            # events.request_success.fire(request_type="stomp", name="connect", response_time=total_time,
            #                             response_length=0)

    def send(self, body, destination, name):
        # print("send")
        start_time = time.time()
        try:
            self.conn.send(destination, body=body)
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request_failure.fire(request_type="stomp", name=name, response_time=total_time, exception=e,
                                        response_length=0)
        else:
            total_time = int((time.time() - start_time) * 1000)
            events.request_success.fire(request_type="stomp", name=name, response_time=total_time, response_length=0)

    def subscribe(self, destination, callback=None, headers=None, name=None):
        # print("subscribe")
        print(str(destination))
        start_time = time.time()
        try:
            self.conn.subscribe(self, destination)
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request_failure.fire(request_type="stomp", name=name, response_time=total_time, exception=e,
                                        response_length=0)
        else:
            total_time = int((time.time() - start_time) * 1000)
            events.request_success.fire(request_type="stomp", name=name, response_time=total_time,
                                        response_length=0)

    def disconnect(self, disconnectCallback=None, headers=None):
        # print("disconnect")
        self.conn.disconnect()


class StompLocust(User):
    """
    This is the abstract Locust class which should be subclassed. It provides an Stomp client
    that can be used to make Stomp requests that will be tracked in Locust's statistics.
    """
    host = "localhost"
    port = 8080
    abstract = True

    def __init__(self, *args, **kwargs):
        super(StompLocust, self).__init__(*args, **kwargs)
        self.client_stomp = StompClient(self.host, self.port)

    def connect(self):
        self.client_stomp.connect()

    def send(self, destination, body, name):
        self.client_stomp.send(self, body, destination, name)

    def disconnect(self):
        self.client_stomp.disconnect()

    def subscribe(self, destination, callback=None, headers=None, name=None):
        print(str(destination))
        self.client_stomp.subscribe(self, destination, name)


def random_str():
    a2z = [chr(i) for i in range(97, 123)]
    return ''.join(random.sample(a2z, 6))


def random_num():
    return random.randint(2000, 5000)


class TestUser(StompLocust, HttpUser):
    host = "https://spring.mquiz.site/"
    port = 80
    min_wait = 100
    max_wait = 1000

    @task
    def Play(self):
        time.sleep(1)
        # HOST ?????? ??????
        response = self.client.post("https://spring.mquiz.site/v1/host/createPlay",
                                    json={"id": "1b448930-1423-425f-8e9d-1a128d4c9922"}, name="Host ????????? ??????")
        # print(json.loads(response.text)['data'])
        pinNum = json.loads(response.text)['data']

        # time.sleep(1)

        # HOST subscribe
        self.client_stomp.connect();
        # time.sleep(2)
        self.client_stomp.subscribe(destination="quiz/" + pinNum, name="Host ??? ??????")

        time.sleep(5) # ??? ?????? ?????????????????? 5??? ?????? ???????????? ?????????

        # CLIENT ?????? ??????(50???)
        clientList = []  # 50?????? ???????????????
        clientCnt = 50
        for i in range(clientCnt):
            clientList.append("client" + str(i))
            print("clientList : " + str(clientList[i]))
            self.client_stomp.subscribe(destination="quiz/" + pinNum, name="CLIENT ??? ??????")
            # time.sleep(1)
            self.client_stomp.send(body=json.dumps({'pinNum': pinNum, 'nickName': clientList[i]}),
                                   destination="/quiz/setnickname", name="CLIENT ????????? ??????")
            time.sleep(0.5) # 0.5??? ???????????? ??????

        # ?????? ??????(10??????)
        for i in range(1, 11):
            self.client_stomp.send(body=json.dumps({'pinNum': pinNum}), destination="/quiz/start",
                                   name="????????? ??????")  # ????????? ??????
            time.sleep(4)
            # CLIENT ?????? ??????
            for j in range(clientCnt):
                self.client_stomp.send(
                    body=json.dumps({'pinNum': pinNum, 'nickName': clientList[j], 'quiz': {
                        "question": "test",
                        "answer": [
                            "num1"
                        ],
                        "useScore": True,
                        "rate": 1,
                        "num": j+1,
                        "choiceList": {
                            "num1": "test",
                            "num4": "test",
                            "num3": "",
                            "num2": ""
                        },
                        "media": {
                            "type": "image",
                            "url": ""
                        },
                        "time": "10",
                        "type": "?????????"
                    }, 'submit': {'quizNum': i, 'answer': ['num1'],
                                  'answerTime': random_num(), 'isAns': True}}),
                    destination="/quiz/submit", name="CLIENT ?????? ??????")  # ?????? ??????
                time.sleep(0.5) # 0.5??? ???????????? ?????? ??????
            time.sleep(5) # 5??? ?????? ?????? ????????? ??????
            self.client_stomp.send(body=json.dumps({'pinNum': pinNum}), destination="/quiz/result",
                                   name="????????? ???")  # ????????? ???
            time.sleep(3)

        self.client_stomp.send(body=json.dumps({'pinNum': pinNum}), destination="/quiz/final",
                               name="?????? ?????????")  # ?????? ?????????
        time.sleep(10)
        self.client_stomp.send(body=json.dumps({'pinNum': pinNum}), destination="/quiz/end",
                               name="?????? ??????")  # ?????? ??????

        self.client.post("https://h0tp3laqa6.execute-api.ap-northeast-3.amazonaws.com/v1/log", json={
            "showid": "showid",
            "email": "Locust",
            "showtitle": "showtitle",
            "playdate": "playdate",
            "quizcount": "quizcount",
            "usercount": "usercount",
            "userdata": [
                {
                    "nickname": "test",
                    "rank": "1",
                    "rankscore": "12345",
                    "correctcount": "12",
                    "iscorrectlist": "0,1,-1"
                }
            ]
        }, name="?????? ??????")

        time.sleep(660) # 1????????? ??? 3?????? ????????? ??????????????? 1?????? 10??? ??????
        # time.sleep(99999999)
        # self.client_stomp.disconnect()
