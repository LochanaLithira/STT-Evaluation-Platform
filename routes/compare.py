"""
/ws/compare — AssemblyAI model A/B endpoint.

Streams the same browser audio to two AssemblyAI realtime models side by side:
  - Universal-3.5 Pro Realtime          (source "u35")
  - AssemblyAI-STT-3.7-Realtime-Agent   (source "s37")
"""
import asyncio
import json

from fastapi import APIRouter, WebSocket

from config import get_assemblyai_key
from pipelines import AssemblyAIPipeline, MODES, PROFILES, EOT_DEFAULT_MS, PhoneLine

router = APIRouter()

# (source id, console label, speech_model)
MODELS = [
    ("u35", "U3.5 Pro", "universal-3-5-pro"),
    ("s37", "STT-3.7 Agent", "assemblyai-stt-3-7-realtime-agent"),
]


@router.websocket("/ws/compare")
async def compare_ws(browser: WebSocket, lang: str = "en", mode: str = "balanced",
                     voice_focus: str = "", profile: str = "live",
                     eot_ms: int = EOT_DEFAULT_MS):
    """
    Audio arrives from the browser as 16 kHz Int16 PCM, 80 ms chunks.

    profile=live (default) runs both models exactly as a live call does:
    the call's connection settings (end-of-turn silences from eot_ms, the
    language prompt and code, partial turns), the browser audio put through
    the phone line (8 kHz mu-law, the call's upsampling and 60 ms sends), and
    {"type":"agent_context","text":...} from the browser sent as the call
    sends what the agent just asked. mode and voice_focus are not sent, as
    on calls. profile=lab is the earlier comparison on AssemblyAI's own
    defaults, with the settings below.

    Every setting is applied to both models so the comparison is like-for-like:
      lang         en | fr | fr-bilingual | auto   (the only toggle in the UI)
      mode         balanced (default) | min_latency | max_accuracy
      voice_focus  "" = off (default) | near-field | far-field
    Defaults: balanced is AssemblyAI's recommended turn detection; voice
    focus off avoids suppressing quiet or distant speech. mode/voice_focus
    stay accepted here for scripted tests. Unknown values fall back to defaults.
    Browser sends binary audio, then {"type":"stop"}; the server answers
    {"type":"done"} once both models have sent their last final.
    """
    await browser.accept()

    try:
        api_key = get_assemblyai_key()
    except RuntimeError as e:
        await browser.send_json({"error": str(e)})
        await browser.close()
        return

    profile = profile if profile in PROFILES else "live"
    eot_ms = max(300, min(5000, int(eot_ms or EOT_DEFAULT_MS)))
    pipelines = [AssemblyAIPipeline(src, label, model, api_key,
                                    mode if mode in MODES else "balanced",
                                    lang, voice_focus, profile=profile, eot_ms=eot_ms)
                 for src, label, model in MODELS]
    # One phone line for both models: they hear exactly the same chunks.
    line = PhoneLine() if profile == "live" else None

    # Connect both at once so neither gets a head start.
    results = await asyncio.gather(*(p.connect() for p in pipelines), return_exceptions=True)
    for p, res in zip(pipelines, results):
        if isinstance(res, Exception):
            print(f"[{p.label}] FAILED: {res}")
            p.ws = None
            try:
                await browser.send_json({"source": p.source, "error": str(res)})
            except Exception:
                pass

    live = [p for p in pipelines if p.ws]
    if not live:
        try:
            await browser.send_json({"error": "All connections failed. Check server console."})
        except Exception:
            pass
        await browser.close()
        return

    async def send_to_browser(msg: dict):
        try:
            await browser.send_json(msg)
        except Exception:
            pass

    async def audio_relay():
        """Forward audio until the browser sends {"type":"stop"} (→ True)
        or disconnects (→ False)."""
        try:
            while True:
                msg = await browser.receive()
                if msg["type"] == "websocket.disconnect":
                    return False
                if msg.get("bytes"):
                    chunks = line.feed(msg["bytes"]) if line else [msg["bytes"]]
                    for chunk in chunks:
                        await asyncio.gather(*(p.send_audio(chunk) for p in live),
                                             return_exceptions=True)
                elif msg.get("text"):
                    try:
                        data = json.loads(msg["text"])
                    except ValueError:
                        continue
                    if data.get("type") == "stop":
                        return True
                    if data.get("type") == "agent_context":
                        sent = await asyncio.gather(
                            *(p.set_agent_context(data.get("text", "")) for p in live),
                            return_exceptions=True)
                        await send_to_browser({"type": "agent_context_sent",
                                               "sent": any(s is True for s in sent)})
        except Exception:
            return False

    relay = asyncio.create_task(audio_relay())
    receivers = [asyncio.create_task(p.receive_loop(send_to_browser)) for p in live]

    try:
        stopped = await relay
        if stopped:
            # Without more audio the models never hear the silence that ends a
            # turn, so the last sentence would stay a partial. Terminate makes
            # each model finalize it; wait for both before telling the browser.
            await asyncio.gather(*(p.finish() for p in live), return_exceptions=True)
            await asyncio.wait(receivers, timeout=8)
            await send_to_browser({"type": "done"})
    finally:
        for p in live:
            await p.close()
        await asyncio.wait(receivers, timeout=3)
        for t in [relay] + receivers:
            t.cancel()
        try:
            await browser.close()
        except Exception:
            pass
