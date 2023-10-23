import asyncio
import time
import re
import hmac
import hashlib
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
import base64


def generate_timestamp():
    """
    Return a millisecond integer timestamp.
    """
    return int(time.time() * 10**3)


def generate_signature(use_rsa_authentication, secret, param_str):
    def generate_hmac():
        hash = hmac.new(
            bytes(secret, "utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        )
        return hash.hexdigest()

    def generate_rsa():
        hash = SHA256.new(param_str.encode("utf-8"))
        encoded_signature = base64.b64encode(
            PKCS1_v1_5.new(RSA.importKey(secret)).sign(
                hash
            )
        )
        return encoded_signature.decode()

    if not use_rsa_authentication:
        return generate_hmac()
    else:
        return generate_rsa()


def identify_ws_method(input_wss_url, wss_dictionary):
    """
    This method matches the input_wss_url with a particular WSS method. This
    helps ensure that, when subscribing to a custom topic, the topic
    subscription message is sent down the correct WSS connection.
    """
    path = re.compile("(wss://)?([^/\s]+)(.*)")
    input_wss_url_path = path.match(input_wss_url).group(3)
    for wss_url, function_call in wss_dictionary.items():
        wss_url_path = path.match(wss_url).group(3)
        if input_wss_url_path == wss_url_path:
            return function_call


def find_index(source, target, key):
    """
    Find the index in source list of the targeted ID.
    """
    return next(i for i, j in enumerate(source) if j[key] == target[key])


def make_private_args(args):
    """
    Exists to pass on the user's arguments to a lower-level class without
    giving the user access to that classes attributes (ie, passing on args
    without inheriting the parent class).
    """
    args.pop("self")
    return args


def make_public_kwargs(private_kwargs):
    public_kwargs = {**private_kwargs}
    public_kwargs.pop("api_key", "")
    public_kwargs.pop("api_secret", "")
    return public_kwargs


def are_connections_connected(active_connections):
    for connection in active_connections:
        if not connection.is_connected():
            return False
    return True


def is_inverse_contract(symbol: str):
    if re.search(r"(USD)([HMUZ]\d\d|$)", symbol):
        return True


def is_usdt_perpetual(symbol: str):
    if symbol.endswith("USDT"):
        return True


def is_usdc_perpetual(symbol: str):
    if symbol.endswith("USDC"):
        return True


def is_usdc_option(symbol: str):
    if re.search(r"[A-Z]{3}-.*-[PC]$", symbol):
        return True


def propagate_future_exception(future):
    if future.exception(): raise future.exception()

def fire_and_forget(coro, loop):
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    future.add_done_callback(propagate_future_exception)
