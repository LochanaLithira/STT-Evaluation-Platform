"""Shared interface implemented by every STT pipeline."""
import bisect
import math
import time
from abc import ABC, abstractmethod
from array import array

# Audio from the browser is 16 kHz mono PCM16 → 32 bytes per millisecond.
BYTES_PER_MS = 16000 * 2 / 1000

# RMS (int16 scale) above which a chunk counts as speech for the fallback
# latency estimate. The browser meter treats <200 as silence, >1000 as speech.
VOICED_RMS = 400


class Pipeline(ABC):
    """
    Every pipeline wraps one provider's streaming STT WebSocket and exposes
    the same four async hooks so routes/compare.py can drive them uniformly:
      - connect()               establish + configure the upstream WS
      - send_audio(bytes)       forward one browser audio chunk
      - receive_loop(send_fn)   consume provider events, normalize, call send_fn(dict)
      - close()                 tear down the upstream WS cleanly

    Latency
    -------
    Subclasses call self._mark_sent(data) right after forwarding each chunk.
    That records, per chunk, the audio-stream offset it ends at and the
    wall-clock time it left this server. Two ways to turn that into latency:

      latency_for_audio_ms(t)  provider gave a word timestamp t (ms into the
                               stream): latency = now − when audio at t was sent.
                               This is the real "spoken → transcribed" delay.
      latency_since_voiced()   provider gave no timestamps: latency = now − when
                               the last voiced chunk was sent. An estimate.

    Both measure from this server, so browser→server transit is not included.
    """

    ws = None

    # ── audio clock ─────────────────────────────────────────────────────────
    def _ensure_clock(self):
        if not hasattr(self, "_chunk_end_ms"):
            self._audio_ms = 0.0
            self._chunk_end_ms = []   # audio offset (ms) at the end of each chunk
            self._chunk_sent_at = []  # time.monotonic() when that chunk was sent
            self._last_voiced_at = None

    def _mark_sent(self, data: bytes):
        self._ensure_clock()
        now = time.monotonic()
        self._audio_ms += len(data) / BYTES_PER_MS
        self._chunk_end_ms.append(self._audio_ms)
        self._chunk_sent_at.append(now)
        if _rms(data) >= VOICED_RMS:
            self._last_voiced_at = now

    def latency_for_audio_ms(self, audio_ms):
        """Latency for a transcript whose last word ends at `audio_ms`."""
        self._ensure_clock()
        if audio_ms is None or not self._chunk_end_ms:
            return None
        i = bisect.bisect_left(self._chunk_end_ms, audio_ms)
        if i >= len(self._chunk_sent_at):
            i = len(self._chunk_sent_at) - 1  # timestamp slightly past what we sent
        return max(0, round((time.monotonic() - self._chunk_sent_at[i]) * 1000))

    def latency_since_voiced(self):
        """Fallback estimate: time since the last chunk containing speech was sent."""
        self._ensure_clock()
        if self._last_voiced_at is None:
            return None
        return round((time.monotonic() - self._last_voiced_at) * 1000)

    # ── provider hooks ──────────────────────────────────────────────────────
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


def _rms(data: bytes) -> float:
    samples = array("h")
    samples.frombytes(data[: len(data) - len(data) % 2])
    if not samples:
        return 0.0
    return math.sqrt(sum(s * s for s in samples) / len(samples))
