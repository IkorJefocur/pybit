import asyncio
import threading
import time
import json
import hmac
import logging
import re
import copy
import inspect
import aiohttp
from . import HTTP
from . import _helpers


logger = logging.getLogger(__name__)


SUBDOMAIN_TESTNET = "stream-testnet"
SUBDOMAIN_MAINNET = "stream"
DOMAIN_MAIN = "bybit"
DOMAIN_ALT = "bytick"

INVERSE_PERPETUAL = "Inverse Perp"
USDT_PERPETUAL = "USDT Perp"
USDC_PERPETUAL = "USDC Perp"
USDC_OPTIONS = "USDC Options"
SPOT = "Spot"


class _WebSocketManager:
    thread_loop = asyncio.new_event_loop()
    thread = threading.Thread(target=thread_loop.run_forever)
    thread.daemon = True

    def __init__(self, callback_function, ws_name,
                 test, domain="", api_key=None, api_secret=None,
                 ping_interval=20, ping_timeout=10, retries=10,
                 restart_on_error=True, trace_logging=False, loop=None,
                 connector=lambda: None, **session_params):

        self.test = test
        self.domain = domain
        self.connector = connector
        self.session_params = session_params

        # Set API keys.
        self.api_key = api_key
        self.api_secret = api_secret

        self.callback = callback_function
        self.ws_name = ws_name
        if api_key:
            self.ws_name += " (Auth)"

        # Setup the callback directory following the format:
        #   {
        #       "topic_name": function
        #   }
        self.callback_directory = {}

        # Record the subscriptions made so that we can resubscribe if the WSS
        # connection is broken.
        self.subscriptions = []

        # Set ping settings.
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.retries = retries

        # Other optional data handling settings.
        self.handle_error = restart_on_error

        # Set initial state, initialize dictionary and connect.
        self.ws = None
        self.session = None
        self._reset()
        self.attempting_connection = False

        self.main_loop = loop
        self.loop = None

    def _on_open(self):
        """
        Log WS open.
        """
        logger.debug(f"WebSocket {self.ws_name} opened.")

    async def _on_message(self, message):
        """
        Parse incoming messages.
        """
        self.callback(message)

    def is_connected(self):
        return self.ws and not self.ws.closed

    @staticmethod
    def _are_connections_connected(active_connections):
        for connection in active_connections:
            if not connection.is_connected():
                return False
        return True

    def _connect(self, url):
        try:
            self.loop = self.main_loop or asyncio.get_running_loop()
        except RuntimeError:
            self.loop = self.thread_loop
        if not self.thread.is_alive():
            self.thread.start()

        connect = self._establish_connection(url)
        asyncio.run_coroutine_threadsafe(connect, self.thread_loop).result()
        _helpers.fire_and_forget(self._listen(), self.thread_loop)

    async def _establish_connection(self, url):
        async def resubscribe_to_topics():
            if not self.subscriptions:
                # There are no subscriptions to resubscribe to, probably
                # because this is a brand new WSS initialisation so there was
                # no previous WSS connection.
                return
            for subscription_message in self.subscriptions:
                await self.ws.send_json(subscription_message)

        self.attempting_connection = True
        self.session = aiohttp.ClientSession(
            connector=self.connector(),
            **self.session_params
        )

        # Set endpoint.
        subdomain = SUBDOMAIN_TESTNET if self.test else SUBDOMAIN_MAINNET
        domain = DOMAIN_MAIN if not self.domain else self.domain
        url = url.format(SUBDOMAIN=subdomain, DOMAIN=domain)
        self.endpoint = url

        self.public_v1_websocket = True if url.endswith("v1") else False
        self.public_v2_websocket = True if url.endswith("v2") else False
        self.private_websocket = True if url.endswith("/spot/ws") else False

        # Attempt to connect for X seconds.
        retries = self.retries
        if retries == 0:
            infinitely_reconnect = True
        else:
            infinitely_reconnect = False

        while True:
            logger.info(f"WebSocket {self.ws_name} attempting connection...")

            try:
                self.ws = await self.session.ws_connect(
                    url=url,
                    heartbeat=self.ping_interval
                )
                break

            except aiohttp.WSServerHandshakeError:
                retries -= 1
                # If connection was not successful, raise error.
                if not infinitely_reconnect and retries <= 0:
                    await self.exit()
                    raise aiohttp.ClientConnectionError(
                        f"WebSocket {self.ws_name} connection failed. "
                        f"Too many connection attempts. "
                        f"pybit will no longer try to reconnect.")

        logger.info(f"WebSocket {self.ws_name} connected")

        # If given an api_key, authenticate.
        if self.api_key and self.api_secret:
            await self._auth()
        await resubscribe_to_topics()

        self.attempting_connection = False

    async def _listen(self):
        while self.is_connected():
            self._on_open()
            try:
                async for message in self.ws:

                    if message.type == aiohttp.WSMsgType.TEXT:
                        await self._on_message(message.json())

                    if message.type == aiohttp.WSMsgType.ERROR:
                        await self._on_error(message.data)
                        break

                    if message.type == aiohttp.WSMsgType.CLOSE:
                        await self._on_close()
                        break

            except asyncio.TimeoutError as error:
                await self._on_error(error)

    async def _auth(self):
        """
        Authorize websocket connection.
        """

        # Generate expires.
        expires = _helpers.generate_timestamp() + 1000

        # Generate signature.
        _val = f"GET/realtime{expires}"
        signature = str(hmac.new(
            bytes(self.api_secret, "utf-8"),
            bytes(_val, "utf-8"), digestmod="sha256"
        ).hexdigest())

        # Authenticate with API.
        await self.ws.send_json({
            "op": "auth",
            "args": [self.api_key, expires, signature]
        })

    async def _on_error(self, error):
        if not self.exited:
            info = str(error) or str(type(error))
            logger.error(
                f"WebSocket {self.ws_name} encountered error: {info}."
            )
            await self.exit()

        # Reconnect.
        if self.handle_error and not self.attempting_connection:
            self._reset()
            await self._establish_connection(self.endpoint)

    async def _on_close(self):
        """
        Log WS close.
        """
        logger.debug(f"WebSocket {self.ws_name} closed.")
        await self.session.close()

    def _reset(self):
        """
        Set state booleans and initialize dictionary.
        """
        self.exited = False
        self.auth = False
        self.data = {}

    async def exit(self):
        """
        Closes the websocket connection.
        """
        if self.ws:
            await self.ws.close()
        await self.session.close()
        self.exited = True

    def subscribe(self, *args, **kwargs):
        coro = self._subscribe(*args, **kwargs)
        if self.loop.is_running():
            _helpers.fire_and_forget(coro, self.loop)
        else:
            self.loop.run_until_complete(coro)

    async def _subscribe(self):
        pass


class _FuturesWebSocketManager(_WebSocketManager):
    def __init__(self, ws_name, **kwargs):
        callback_function = kwargs.pop("callback_function") if \
            kwargs.get("callback_function") else self._handle_incoming_message
        super().__init__(callback_function, ws_name, **kwargs)

        self.private_topics = ["position", "execution", "order", "stop_order",
                               "wallet"]

        self.symbol_wildcard = "*"
        self.symbol_separator = "|"

    async def _subscribe(self, topic, callback, symbol=None):
        if symbol is None:
            symbol = []
        elif type(symbol) == str:
            symbol = [symbol]

        async def prepare_subscription_args(list_of_symbols):
            """
            Prepares the topic for subscription by formatting it with the
            desired symbols.
            """
            async def get_all_usdt_symbols():
                http = _FuturesHTTPManager()
                query_symbol_response = (await http.query_symbol())["result"]
                for symbol_spec in query_symbol_response:
                    symbol = symbol_spec["name"]
                    if symbol.endswith("USDT"):
                        list_of_symbols.append(symbol)
                return list_of_symbols

            if topic in self.private_topics:
                # private topics do not support filters
                return [topic]
            elif list_of_symbols == self.symbol_wildcard or not list_of_symbols:
                # different WSS URL support may or may not support the
                # wildcard; for USDT, we need to manually get all symbols
                if self.ws_name != USDT_PERPETUAL:
                    return [topic.format(self.symbol_wildcard)]
                list_of_symbols = await get_all_usdt_symbols()

            topics = []
            for symbol in list_of_symbols:
                topics.append(topic.format(symbol))
            return topics

        subscription_args = await prepare_subscription_args(symbol)
        self._check_callback_directory(subscription_args)

        while not self.is_connected():
            # Wait until the connection is open before subscribing.
            await asyncio.sleep(0.1)

        subscription_message = {
            "op": "subscribe",
            "args": subscription_args
        }
        await self.ws.send_json(subscription_message)
        self.subscriptions.append(subscription_message)
        self._set_callback(topic, callback)

    def _initialise_local_data(self, topic):
        # Create self.data
        try:
            self.data[topic]
        except KeyError:
            self.data[topic] = []

    def _process_delta_orderbook(self, message, topic):
        self._initialise_local_data(topic)

        # Record the initial snapshot.
        if "snapshot" in message["type"]:
            if type(message["data"]) is list:
                self.data[topic] = message["data"]
            elif message["data"].get("order_book"):
                self.data[topic] = message["data"]["order_book"]
            elif message["data"].get("orderBook"):
                self.data[topic] = message["data"]["orderBook"]

        # Make updates according to delta response.
        elif "delta" in message["type"]:

            # Delete.
            for entry in message["data"]["delete"]:
                index = _helpers.find_index(self.data[topic], entry, "id")
                self.data[topic].pop(index)

            # Update.
            for entry in message["data"]["update"]:
                index = _helpers.find_index(self.data[topic], entry, "id")
                self.data[topic][index] = entry

            # Insert.
            for entry in message["data"]["insert"]:
                self.data[topic].append(entry)

    def _process_delta_instrument_info(self, message, topic):
        self._initialise_local_data(topic)

        # Record the initial snapshot.
        if "snapshot" in message["type"]:
            self.data[topic] = message["data"]

        # Make updates according to delta response.
        elif "delta" in message["type"]:
            # Update.
            for update in message["data"]["update"]:
                for key, value in update.items():
                    self.data[topic][key] = value

    def _process_auth_message(self, message):
        # If we get successful futures auth, notify user
        if message.get("success") is True:
            logger.debug(f"Authorization for {self.ws_name} successful.")
            self.auth = True
        # If we get unsuccessful auth, notify user.
        elif message.get("success") is False:
            logger.warn(f"Authorization for {self.ws_name} failed. Please "
                         f"check your API keys and restart.")

    def _process_subscription_message(self, message):
        try:
            sub = message["request"]["args"]
        except KeyError:
            sub = message["data"]["successTopics"]  # USDC private sub format

        # If we get successful futures subscription, notify user
        if message.get("success") is True:
            logger.debug(f"Subscription to {sub} successful.")
        # Futures subscription fail
        elif message.get("success") is False:
            response = message["ret_msg"]
            logger.error("Couldn't subscribe to topic."
                         f"Error: {response}.")
            self._pop_callback(sub[0])

    def _process_normal_message(self, message):
        topic = message["topic"]
        if "orderBook" in topic:
            self._process_delta_orderbook(message, topic)
            callback_data = copy.deepcopy(message)
            callback_data["type"] = "snapshot"
            callback_data["data"] = self.data[topic]
        elif "instrument_info" in topic:
            self._process_delta_instrument_info(message, topic)
            callback_data = copy.deepcopy(message)
            callback_data["type"] = "snapshot"
            callback_data["data"] = self.data[topic]
        else:
            callback_data = message
        callback_function = self._get_callback(topic)
        callback_function(callback_data)

    def _handle_incoming_message(self, message):
        def is_auth_message():
            if message.get("request", {}).get("op") == "auth":
                return True
            else:
                return False

        def is_subscription_message():
            if message.get("request", {}).get("op") == "subscribe":
                return True
            else:
                return False

        if is_auth_message():
            self._process_auth_message(message)
        elif is_subscription_message():
            self._process_subscription_message(message)
        else:
            self._process_normal_message(message)

    def custom_topic_stream(self, topic, callback):
        return self.subscribe(topic=topic, callback=callback)

    def _extract_topic(self, topic_string):
        """
        Regex to return the topic without the symbol.
        """
        def is_usdc_private_topic():
            if re.search(r".*\..*\..*\.", topic_string):
                return True

        if topic_string in self.private_topics or is_usdc_private_topic():
            return topic_string
        topic_without_symbol = re.match(r".*(\..*|)(?=\.)", topic_string)
        return topic_without_symbol[0]

    @staticmethod
    def _extract_symbol(topic_string):
        """
        Regex to return the symbol without the topic.
        """
        symbol_without_topic = re.search(r"(?!.*\.)[A-Z*|]*$", topic_string)
        return symbol_without_topic[0]

    def _check_callback_directory(self, topics):
        for topic in topics:
            if topic in self.callback_directory:
                raise Exception(f"You have already subscribed to this topic: "
                                f"{topic}")

    def _set_callback(self, topic, callback_function):
        def callback(*args, **kwargs):
            run_callback = lambda: callback_function(*args, **kwargs)
            if inspect.iscoroutinefunction(callback_function):
                _helpers.fire_and_forget(run_callback(), self.loop)
            else:
                self.loop.call_soon_threadsafe(run_callback)

        topic = self._extract_topic(topic)
        self.callback_directory[topic] = callback

    def _get_callback(self, topic):
        topic = self._extract_topic(topic)
        return self.callback_directory[topic]

    def _pop_callback(self, topic):
        topic = self._extract_topic(topic)
        self.callback_directory.pop(topic)


class _USDCWebSocketManager(_FuturesWebSocketManager):
    def __init__(self, ws_name, **kwargs):
        super().__init__(
            ws_name, callback_function=self._handle_incoming_message, **kwargs)

    def _handle_incoming_message(self, message):
        def is_auth_message():
            if message.get("type") == "AUTH_RESP":
                return True
            else:
                return False

        def is_subscription_message():
            if message.get("request", {}).get("op") == "subscribe" or \
                    message.get("type") == "COMMAND_RESP":  # Private sub format
                return True
            else:
                return False

        if is_auth_message():
            self._process_auth_message(message)
        elif is_subscription_message():
            self._process_subscription_message(message)
        else:
            self._process_normal_message(message)


class _USDCOptionsWebSocketManager(_USDCWebSocketManager):
    def _process_delta_orderbook(self, message, topic):
        self._initialise_local_data(topic)

        # Record the initial snapshot.
        if "NEW" in message["data"]["dataType"]:
            self.data[topic] = message["data"]["orderBook"]

        # Make updates according to delta response.
        elif "CHANGE" in message["data"]["dataType"]:

            # Delete.
            for entry in message["data"]["delete"]:
                index = _helpers.find_index(self.data[topic], entry, "price")
                self.data[topic].pop(index)

            # Update.
            for entry in message["data"]["update"]:
                index = _helpers.find_index(self.data[topic], entry, "price")
                self.data[topic][index] = entry

            # Insert.
            for entry in message["data"]["insert"]:
                self.data[topic].append(entry)

    def _process_normal_message(self, message):
        topic = message["topic"]
        if "delta.orderbook" in topic:
            self._process_delta_orderbook(message, topic)
            callback_data = copy.deepcopy(message)
            callback_data["data"]["dataType"] = "NEW"
            for key in ["delete", "update", "insert"]:
                callback_data["data"].pop(key, "")
            callback_data["data"]["orderBook"] = self.data[topic]
        else:
            callback_data = message
        callback_function = self._get_callback(topic)
        callback_function(callback_data)


class _SpotWebSocketManager(_WebSocketManager):
    def __init__(self, ws_name, **kwargs):
        super().__init__(self._handle_incoming_message, ws_name, **kwargs)

    async def _subscribe(self, topic, callback):
        """
        Formats and sends the subscription message, given a topic. Saves the
        provided callback function, to be called by incoming messages.
        """
        def format_topic_with_multiple_symbols(topic):
            symbol = topic["symbol"]
            if type(symbol) == str:
                return topic
            elif type(symbol) == list:
                symbol_string = ""
                for item in symbol:
                    symbol_string += item + ","
                symbol_string = symbol_string[:-1]
                symbol = symbol_string
            else:
                raise Exception(f"Could not recognise symbol: "
                                f"({type(symbol)}) {symbol}")
            topic["symbol"] = symbol
            return topic

        if self.private_websocket:
            # Spot private topics don't need a subscription message
            self._set_callback(topic, callback, private_websocket=True)
            return

        conformed_topic = self._conform_topic(topic)

        if conformed_topic in self.callback_directory:
            raise Exception(f"You have already subscribed to this topic: "
                            f"{topic}")

        if self.public_v1_websocket:
            topic = format_topic_with_multiple_symbols(topic)
        await self.ws.send_json(topic)
        topic = conformed_topic
        self._set_callback(topic, callback)

    def _handle_incoming_message(self, message):
        def is_ping_message():
            if type(message) == dict and message.get("ping"):
                # This is an unconventional ping message which looks like:
                # {"ping":1641489450001}
                # For now, we will not worry about responding to this,
                # as websocket-client automatically sends conventional ping
                # frames every 30 seconds, which successfully receive a pong
                # frame response.
                # https://websocket-client.readthedocs.io/en/latest/examples.html#ping-pong-usage
                return True
            else:
                return False

        def is_auth_message():
            if type(message) == dict and message.get("auth"):
                return True
            else:
                return False

        def is_subscription_message():
            if type(message) == dict and \
                    (message.get("event") == "sub" or message.get("code")):
                return True
            else:
                return False

        def process_delta_orderbook():
            # Create self.data
            book_sides = {"b": message["data"][0]["b"],
                          "a": message["data"][0]["a"]}
            try:
                self.data[topic]
            except KeyError:
                self.data[topic] = book_sides

            # Record the initial snapshot.
            if len(book_sides["b"]) == 200 and len(book_sides["a"]) == 200:
                self.data[topic] = book_sides
                return

            # Make updates according to the response.
            for side, entries in book_sides.items():
                for entry in entries:
                    # Delete.
                    if float(entry[1]) == 0:
                        index = _helpers.find_index(
                            self.data[topic][side], entry, 0)
                        self.data[topic][side].pop(index)
                        continue

                    # Insert.
                    price_level_exists = \
                        entry[0] in \
                        [level[0] for level in self.data[topic][side]]
                    if not price_level_exists:
                        self.data[topic][side].append(entry)
                        continue

                    # Update.
                    qty_changed = entry[1] != next(
                        level[1] for level in self.data[topic][side] if
                        level[0] == entry[0])
                    if price_level_exists and qty_changed:
                        index = _helpers.find_index(
                            self.data[topic][side], entry, 0)
                        self.data[topic][side][index] = entry
                        continue

        if is_ping_message():
            return

        # Check auth
        if is_auth_message():
            # If we get successful spot auth, notify user
            if message.get("auth") == "success":
                logger.debug(f"Authorization for {self.ws_name} successful.")
                self.auth = True
            # If we get unsuccessful auth, notify user.
            elif message.get("auth") == "fail":
                logger.debug(f"Authorization for {self.ws_name} failed. Please "
                             f"check your API keys and restart.")

        # Check subscription
        elif is_subscription_message():
            # If we get successful spot subscription, notify user
            if message.get("success") is True:
                sub = self._conform_topic(message)
                logger.debug(f"Subscription to {sub} successful.")
            # Spot subscription fail
            elif message.get("code") != "0":
                # There is no way to confirm the incoming message is related to
                #  any particular subscription message sent by the client, as
                #  the incoming message only includes an error code and
                #  message. Maybe a workaround could be developed in the future.
                logger.debug(f"Subscription failed: {message}")
                raise Exception("Spot subscription failed.")

        else:  # Standard topic push
            if self.private_websocket:
                for item in message:
                    topic_name = item["e"]
                    if self.callback_directory.get(topic_name):
                        callback_function = self.callback_directory[topic_name]
                        callback_function(item)
            else:
                topic = self._conform_topic(message)
                if "diffDepth" in topic:
                    process_delta_orderbook()
                    callback_data = copy.deepcopy(message)
                    callback_data["data"][0]["b"] = self.data[topic]["b"]
                    callback_data["data"][0]["a"] = self.data[topic]["a"]
                else:
                    callback_data = message
                callback_function = self.callback_directory[topic]
                callback_function(callback_data)

    @staticmethod
    def _conform_topic(topic):
        """
        For spot API. Due to the fact that the JSON received in update
        messages does not include a simple "topic" key, and parameters all
        have their own separate keys, we need to compare the entire JSON.
        Therefore, we need to strip the JSON of any unnecessary keys, cast some
        values, and dump the JSON with sort_keys.
        """
        if isinstance(topic, str):
            topic = json.loads(topic)
        else:
            topic = copy.deepcopy(topic)
        topic.pop("event", "")
        topic.pop("symbolName", "")
        topic.pop("symbol", "")
        topic["params"].pop("realtimeInterval", "")
        topic["params"].pop("symbolName", "")
        topic["params"].pop("symbol", "")
        if topic["params"].get("klineType"):
            topic["topic"] += "_" + topic["params"].get("klineType")
            topic["params"].pop("klineType")
        if topic["params"].get("binary"):
            binary = topic["params"]["binary"]
            binary = False if binary == "false" else "true"
            topic["params"]["binary"] = binary
        if topic["params"].get("dumpScale"):
            topic["params"]["dumpScale"] = int(topic["params"]["dumpScale"])
        topic.pop("data", "")
        topic.pop("f", "")
        topic.pop("sendTime", "")
        topic.pop("shared", "")
        return json.dumps(topic, sort_keys=True, separators=(",", ":"))

    def _set_callback(self, topic, callback_function, private_websocket=False):
        if not private_websocket:
            topic = self._conform_topic(topic)
        self.callback_directory[topic] = callback_function

    def _get_callback(self, topic):
        topic = self._conform_topic(topic)
        return self.callback_directory[topic]

    def _pop_callback(self, topic):
        topic = self._conform_topic(topic)
        self.callback_directory.pop(topic)

    def _check_callback_directory(self, topics):
        for topic in topics:
            if topic in self.callback_directory:
                raise Exception(f"You have already subscribed to this topic: "
                                f"{topic}")
