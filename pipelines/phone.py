"""The phone line, as live calls deliver a caller's voice to AssemblyAI.

On a call, Asterisk sends the caller's audio as 8 kHz mu-law (G.711) RTP.
The platform decodes it to PCM16, upsamples it to 16 kHz with
audioop.ratecv (state kept across frames), and buffers the 20 ms frames
until there are at least 1,600 bytes (50 ms) before sending, at most
16,000 bytes (500 ms) per message (ai-voice-agent-api: media_core.py and
call_handler.py). With 20 ms frames that sends 60 ms chunks.

PhoneLine puts the browser's clean 16 kHz microphone audio through the same
path: down to 8 kHz, mu-law encode and decode, the same ratecv upsample, the
same framing and buffering. What it cannot add is the caller's own network
(mobile codecs, packet loss), so real call audio can still be a little worse.
"""
# Built in up to Python 3.12; on 3.13+ requirements.txt installs audioop-lts,
# which provides the same module.
import audioop

WIDTH = 2                # PCM16
LINE_RATE = 8000         # RTP_SAMPLE_RATE on calls
PIPELINE_RATE = 16000    # PIPELINE_SAMPLE_RATE on calls
FRAME_BYTES = 640        # one 20 ms RTP frame at 16 kHz, after upsampling
MIN_SEND = 1_600         # AssemblyAI rejects < 50 ms
MAX_SEND = 16_000        # ... and > 1000 ms; calls cap each send at 500 ms


class PhoneLine:
    """One caller's audio, frame by frame, as a call sends it."""

    def __init__(self):
        self._down = None          # ratecv state, 16 kHz → 8 kHz (the line)
        self._up = None            # ratecv state, 8 kHz → 16 kHz (as on calls)
        self._frames = bytearray()  # upsampled audio not yet cut into frames
        self._buf = bytearray()     # the call's _aai_buf

    def feed(self, pcm16k: bytes) -> list[bytes]:
        """Browser audio in; the chunks a call would send to AssemblyAI out."""
        line, self._down = audioop.ratecv(pcm16k, WIDTH, 1, PIPELINE_RATE, LINE_RATE, self._down)
        ulaw = audioop.lin2ulaw(line, WIDTH)
        pcm8k = audioop.ulaw2lin(ulaw, WIDTH)
        pcm, self._up = audioop.ratecv(pcm8k, WIDTH, 1, LINE_RATE, PIPELINE_RATE, self._up)
        self._frames.extend(pcm)
        out: list[bytes] = []
        while len(self._frames) >= FRAME_BYTES:
            frame = bytes(self._frames[:FRAME_BYTES])
            del self._frames[:FRAME_BYTES]
            out += self._send_rule(frame)
        return out

    def _send_rule(self, frame: bytes) -> list[bytes]:
        """call_handler's buffering, line for line: buffer to >= 1,600 bytes,
        send in slices of at most 16,000, keep a remainder under 1,600."""
        self._buf.extend(frame)
        if len(self._buf) < MIN_SEND:
            return []
        buf = bytes(self._buf)
        end = (len(buf) // MAX_SEND) * MAX_SEND
        if end == 0:
            end = len(buf)
        elif len(buf) - end >= MIN_SEND:
            end = len(buf)
        del self._buf[:end]
        return [buf[i:i + MAX_SEND] for i in range(0, end, MAX_SEND)]
