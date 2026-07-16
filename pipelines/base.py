"""Shared interface implemented by every STT pipeline."""
from abc import ABC, abstractmethod


class Pipeline(ABC):
    """
    Every pipeline wraps one provider's streaming STT WebSocket and exposes
    the same four async hooks so routes/compare.py can drive them uniformly:
      - connect()               establish + configure the upstream WS
      - send_audio(bytes)       forward one browser audio chunk
      - receive_loop(send_fn)   consume provider events, normalize, call send_fn(dict)
      - close()                 tear down the upstream WS cleanly
    """

    ws = None

    @abstractmethod
    async def connect(self):
        ...

    @abstractmethod
    async def send_audio(self, data: bytes):
        ...

    @abstractmethod
    async def receive_loop(self, send_fn):
        ...

    @abstractmethod
    async def close(self):
        ...
