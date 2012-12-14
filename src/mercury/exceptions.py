class MercuryException(Exception):
    pass

class MercurialNotFound(MercuryException):
    pass

class AlreadyConnected(MercuryException):
    pass

class ProtocolError(MercuryException):
    pass

class CapabilityError(MercuryException):
    pass

class ChannelError(MercuryException):
    pass

class PipeError(MercuryException):
    def __init__(self, ret, err):
        self.ret = ret
        self.err = err

    def __str__(self):
        return self.err

class CommandError(MercuryException):
    def __init__(self, args, ret, out, err):
        self.args = args
        self.ret = ret
        self.out = out
        self.err = err

    def __str__(self):
        return self.err

class RemoteRepositoryError(MercuryException):
    pass

class BadBinaryHunk(MercuryException):
    pass

class BadFieldError(MercuryException):
    pass

class NotARepositoryError(MercuryException):
    pass

class BadBinaryDeltaError(MercuryException):
    pass
