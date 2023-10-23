class UnauthorizedExceptionError(Exception):
    pass


class InvalidChannelTypeError(Exception):
    pass


class TopicMismatchError(Exception):
    pass


class FailedRequestError(Exception):
    """
    Exception raised for failed requests.

    Attributes:
        request -- The original request that caused the error.
        message -- Explanation of the error.
        status_code -- The code number returned.
        time -- The time of the error.
        resp_headers -- The response headers from API. None, if the request caused an error locally.
    """

    def __init__(self, request, message, status_code, time, resp_headers):
        self.request = request
        self.message = message
        self.status_code = status_code
        self.time = time
        self.resp_headers = resp_headers
        super().__init__(
            f"{message.capitalize()} (ErrCode: {status_code}) (ErrTime: {time})"
            f".\nRequest → {request}."
        )


class InvalidRequestErrorType(type):
    def __getitem__(cls, status_code):
        if status_code not in cls.status_types:
            class RequestStatusError(cls):
                pass
            RequestStatusError.status_code = status_code
            cls.status_types[status_code] = RequestStatusError
        return cls.status_types[status_code]

class InvalidRequestError(Exception, metaclass = InvalidRequestErrorType):
    """
    Exception raised for returned Bybit errors.

    Attributes:
        request -- The original request that caused the error.
        message -- Explanation of the error.
        status_code -- The code number returned.
        time -- The time of the error.
        resp_headers -- The response headers from API. None, if the request caused an error locally.
    """

    status_types = {}
    status_code = 0

    def __new__(cls, request, message, status_code, time, resp_headers):
        return super().__new__(cls[status_code])

    def __init__(self, request, message, status_code, time, resp_headers):
        self.request = request
        self.message = message
        self.time = time
        self.resp_headers = resp_headers
        super().__init__(
            f"{message} (ErrCode: {status_code}) (ErrTime: {time})"
            f".\nRequest → {request}."
        )
