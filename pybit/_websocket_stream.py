import asyncio
import json
import logging
import copy
from uuid import uuid4
import aiohttp
from . import _helpers


logger = logging.getLogger(__name__)


SUBDOMAIN_TESTNET = "stream-testnet"
SUBDOMAIN_MAINNET = "stream"
DOMAIN_MAIN = "bybit"
DOMAIN_ALT = "bytick"


class _WebSocketManager:
    def __init__(
        self,
        callback_function,
        ws_name,
        testnet,
        domain="",
        rsa_authentication=False,
        api_key=None,
        api_secret=None,
        ping_interval=20,
        ping_timeout=10,
        retries=10,
        restart_on_error=True,
        trace_logging=False,
        private_auth_expire=1,
        loop=None,
        connector=lambda: None
    ):
        self.testnet = testnet
        self.domain = domain
        self.connector = connector
        self.rsa_authentication = rsa_authentication

        # Set API keys.
        self.api_key = api_key
        self.api_secret = api_secret

        self.callback = callback_function
        self.ws_name = ws_name
        if api_key:
            self.ws_name += " (Auth)"
        
        # Delta time for private auth expiration in seconds
        self.private_auth_expire = private_auth_expire

        # Setup the callback directory following the format:
        #   {
        #       "topic_name": function
        #   }
        self.callback_directory = {}

        # Record the subscriptions made so that we can resubscribe if the WSS
        # connection is broken.
        self.subscriptions = {}

        # Set ping settings.
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.custom_ping_message = {"op": "ping"}
        self.retries = retries

        # Other optional data handling settings.
        self.handle_error = restart_on_error

        # Set initial state, initialize dictionary and connect.
        self.ws = None
        self.session = None
        self._reset()
        self.attempting_connection = False
        self.connected = asyncio.Event()

        self.main_loop = loop

    async def _on_open(self):
        """
        WS open.
        """
        logger.debug(f"WebSocket {self.ws_name} opened.")

    async def _on_message(self, message):
        """
        Parse incoming messages.
        """
        if not self._is_custom_pong(message):
            await self.callback(message)

    def is_connected(self):
        return self.connected.is_set()

    def _connect(self, url):
        """
        Open websocket.
        """
        self.loop = self.main_loop or asyncio.get_running_loop()
        _helpers.fire_and_forget(self._full_connect(url), self.loop)

    async def _full_connect(self, url):
        await self._establish_connection(url)
        await self._listen()

    async def _establish_connection(self, url):
        async def resubscribe_to_topics():
            if not self.subscriptions:
                # There are no subscriptions to resubscribe to, probably
                # because this is a brand new WSS initialisation so there was
                # no previous WSS connection.
                return

            for req_id, subscription_message in self.subscriptions.items():
                await self.ws.send_json(subscription_message)

        self.attempting_connection = True
        self.session = aiohttp.ClientSession(connector=self.connector())

        # Set endpoint.
        subdomain = SUBDOMAIN_TESTNET if self.testnet else SUBDOMAIN_MAINNET
        domain = DOMAIN_MAIN if not self.domain else self.domain
        url = url.format(SUBDOMAIN=subdomain, DOMAIN=domain)
        self.endpoint = url

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
                    heartbeat=self.ping_interval,
                    receive_timeout=self.ping_timeout + self.ping_interval,
                    autoping=False
                )
                break

            except (
                aiohttp.WSServerHandshakeError,
                aiohttp.ClientConnectionError
            ):
                retries -= 1
                # If connection was not successful, raise error.
                if not infinitely_reconnect and retries <= 0:
                    await self.exit()
                    raise RuntimeError(
                        f"WebSocket {self.ws_name} "
                        f"({self.endpoint}) connection "
                        f"failed. Too many connection attempts. pybit will no "
                        f"longer try to reconnect."
                    )

        logger.info(f"WebSocket {self.ws_name} connected")

        # If given an api_key, authenticate.
        if self.api_key and self.api_secret:
            await self._auth()

        await resubscribe_to_topics()

        self.attempting_connection = False
        self.connected.set()

    async def _listen(self):
        while self.connected.is_set():
            await self._on_open()
            try:
                while True:
                    message = await self.ws.receive()

                    if message.type == aiohttp.WSMsgType.PING:
                        await self.ws.pong(message.data)

                    if message.type == aiohttp.WSMsgType.PONG:
                        await self._on_pong()

                    if message.type == aiohttp.WSMsgType.TEXT:
                        await self._on_message(message.json())

                    if message.type == aiohttp.WSMsgType.ERROR:
                        await self._on_error()
                        break

                    if message.type == aiohttp.WSMsgType.CLOSE:
                        await self._on_close(message.data)
                        break

                    if message.type == aiohttp.WSMsgType.CLOSED:
                        break

            except asyncio.TimeoutError as error:
                await self._on_error(repr(error))

    async def _auth(self):
        """
        Prepares authentication signature per Bybit API specifications.
        """

        expires = _helpers.generate_timestamp() + (self.private_auth_expire * 1000)

        param_str = f"GET/realtime{expires}"

        signature = _helpers.generate_signature(
            self.rsa_authentication, self.api_secret, param_str
        )

        # Authenticate with API.
        await self.ws.send_json(
            {"op": "auth", "args": [self.api_key, expires, signature]}
        )

    async def _on_error(self, info = None):
        """
        Exit on errors and raise exception, or attempt reconnect.
        """
        if not self.exited:
            logger.error(
                f"WebSocket {self.ws_name} encountered error: {info or ''}."
            )
            await self.exit()

        # Reconnect.
        if self.handle_error and not self.attempting_connection:
            self._reset()
            await self._establish_connection(self.endpoint)

    async def _on_close(self, code):
        """
        WS close.
        """
        if code == aiohttp.WSCloseCode.OK:
            logger.debug(f"WebSocket {self.ws_name} closed.")
            await self.exit()

        elif code in (
            aiohttp.WSCloseCode.GOING_AWAY,
            aiohttp.WSCloseCode.INTERNAL_ERROR,
            aiohttp.WSCloseCode.SERVICE_RESTART,
            aiohttp.WSCloseCode.TRY_AGAIN_LATER,
            aiohttp.WSCloseCode.BAD_GATEWAY
        ):
            await self._on_error(str(code))

        else:
            logger.error(f"WebSocket {self.ws_name} closed with error: {code}.")
            await self.exit()

    async def _on_pong(self):
        """
        Sends a custom ping upon the receipt of the pong frame.

        The websocket library will automatically send ping frames. However, to
        ensure the connection to Bybit stays open, we need to send a custom
        ping message separately from this. When we receive the response to the
        ping frame, this method is called, and we will send the custom ping as
        a normal OPCODE_TEXT message and not an OPCODE_PING.
        """
        await self._send_custom_ping()

    async def _send_custom_ping(self):
        await self.ws.send_json(self.custom_ping_message)

    @staticmethod
    def _is_custom_pong(message):
        """
        Referring to OPCODE_TEXT pongs from Bybit, not OPCODE_PONG.
        """
        if message.get("ret_msg") == "pong" or message.get("op") == "pong":
            return True

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
        self.exited = True
        self.connected.clear()
        if self.ws and not self.ws.closed:
            await self.ws.close()
        await self.session.close()

    def subscribe(self, *args, **kwargs):
        _helpers.fire_and_forget(self._subscribe(*args, **kwargs), self.loop)

    async def _subscribe(self, *args, **kwargs):
        pass

    def subscribe(self, *args, **kwargs):
        coro = self._subscribe(*args, **kwargs)
        if self.loop.is_running():
            _helpers.fire_and_forget(coro, self.loop)
        else:
            self.loop.run_until_complete(coro)

    async def _subscribe(self):
        pass


class _V5WebSocketManager(_WebSocketManager):
    def __init__(self, ws_name, **kwargs):
        callback_function = (
            kwargs.pop("callback_function")
            if kwargs.get("callback_function")
            else self._handle_incoming_message
        )
        super().__init__(callback_function, ws_name, **kwargs)

        self.private_topics = [
            "position",
            "execution",
            "order",
            "wallet",
            "greeks",
        ]

    async def _subscribe(
        self,
        topic: str,
        callback,
        symbol: (str, list) = False
    ):

        def prepare_subscription_args(list_of_symbols):
            """
            Prepares the topic for subscription by formatting it with the
            desired symbols.
            """

            if topic in self.private_topics:
                # private topics do not support filters
                return [topic]

            topics = []
            for single_symbol in list_of_symbols:
                topics.append(topic.format(symbol=single_symbol))
            return topics

        if type(symbol) == str:
            symbol = [symbol]

        subscription_args = prepare_subscription_args(symbol)
        self._check_callback_directory(subscription_args)

        req_id = str(uuid4())

        subscription_message = {
            "op": "subscribe", "req_id": req_id, "args": subscription_args
        }
        # Wait until the connection is open before subscribing.
        await self.connected.wait()

        await self.ws.send_json(subscription_message)
        self.subscriptions[req_id] = subscription_message
        for topic in subscription_args:
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
            self.data[topic] = message["data"]
            return

        # Make updates according to delta response.
        book_sides = {"b": message["data"]["b"], "a": message["data"]["a"]}

        for side, entries in book_sides.items():
            for entry in entries:
                # Delete.
                if float(entry[1]) == 0:
                    index = _helpers.find_index(
                        self.data[topic][side], entry, 0
                    )
                    self.data[topic][side].pop(index)
                    continue

                # Insert.
                price_level_exists = entry[0] in [
                    level[0] for level in self.data[topic][side]
                ]
                if not price_level_exists:
                    self.data[topic][side].append(entry)
                    continue

                # Update.
                qty_changed = entry[1] != next(
                    level[1]
                    for level in self.data[topic][side]
                    if level[0] == entry[0]
                )
                if price_level_exists and qty_changed:
                    index = _helpers.find_index(
                        self.data[topic][side], entry, 0
                    )
                    self.data[topic][side][index] = entry
                    continue

    def _process_delta_ticker(self, message, topic):
        self._initialise_local_data(topic)

        # Record the initial snapshot.
        if "snapshot" in message["type"]:
            self.data[topic] = message["data"]

        # Make updates according to delta response.
        elif "delta" in message["type"]:
            for key, value in message["data"].items():
                self.data[topic][key] = value

    async def _process_auth_message(self, message):
        # If we get successful futures auth, notify user
        if message.get("success") is True:
            logger.debug(f"Authorization for {self.ws_name} successful.")
            self.auth = True
        # If we get unsuccessful auth, notify user.
        elif message.get("success") is False or message.get("type") == "error":
            raise Exception(
                f"Authorization for {self.ws_name} failed. Please check your "
                f"API keys and restart. Raw error: {message}"
            )

    async def _process_subscription_message(self, message):
        if message.get("req_id"):
            sub = self.subscriptions[message["req_id"]]["args"][0]
        else:
            # if req_id is not supported, guess that the last subscription
            # sent was successful
            sub = list(self.subscriptions.values())[0]["args"][0]

        # If we get successful futures subscription, notify user
        if message.get("success") is True:
            logger.debug(f"Subscription to {sub} successful.")
        # Futures subscription fail
        elif message.get("success") is False:
            response = message["ret_msg"]
            logger.error("Couldn't subscribe to topic." f"Error: {response}.")
            self._pop_callback(sub[0])

    async def _process_normal_message(self, message):
        topic = message["topic"]
        if "orderbook" in topic:
            self._process_delta_orderbook(message, topic)
            callback_data = copy.deepcopy(message)
            callback_data["type"] = "snapshot"
            callback_data["data"] = self.data[topic]
        elif "tickers" in topic:
            self._process_delta_ticker(message, topic)
            callback_data = copy.deepcopy(message)
            callback_data["type"] = "snapshot"
            callback_data["data"] = self.data[topic]
        else:
            callback_data = message
        callback_function = self._get_callback(topic)
        await callback_function(callback_data)

    async def _handle_incoming_message(self, message):
        def is_auth_message():
            if (
                message.get("op") == "auth"
                or message.get("type") == "AUTH_RESP"
            ):
                return True
            else:
                return False

        def is_subscription_message():
            if (
                message.get("op") == "subscribe"
                or message.get("type") == "COMMAND_RESP"
            ):
                return True
            else:
                return False

        if is_auth_message():
            await self._process_auth_message(message)
        elif is_subscription_message():
            await self._process_subscription_message(message)
        else:
            await self._process_normal_message(message)

    def _check_callback_directory(self, topics):
        for topic in topics:
            if topic in self.callback_directory:
                raise Exception(
                    f"You have already subscribed to this topic: " f"{topic}"
                )

    def _set_callback(self, topic, callback_function):
        self.callback_directory[topic] = callback_function

    def _get_callback(self, topic):
        return self.callback_directory[topic]

    def _pop_callback(self, topic):
        self.callback_directory.pop(topic)
