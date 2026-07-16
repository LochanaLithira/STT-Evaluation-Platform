"""
/ws/compare4 — 4-way STT comparison endpoint.

AssemblyAI U3 Pro + Voxtral + Soniox + Gladia Solaria.
"""
import asyncio

from fastapi import APIRouter, WebSocket

from config import get_assemblyai_key, get_mistral_key, get_soniox_key, get_gladia_key
from pipelines import (
    AssemblyAIPipeline, VoxtralPipeline, SonioxPipeline, GladiaPipeline,
)

router = APIRouter()


async def _optional_key(browser: WebSocket, getter, src: str):
    """Fetch an optional key; on failure, warn but let the comparison continue."""
    try:
        return getter()
    except RuntimeError as e:
        try:
            await browser.send_json({"source": src, "error": str(e)})
        except Exception:
            pass
        return None


@router.websocket("/ws/compare4")
async def compare4_ws(browser: WebSocket, lang: str = "en"):
    """
    4-way comparison: AssemblyAI U3 Pro + Voxtral + Soniox + Gladia Solaria.
    Audio arrives from the browser resampled to 16 kHz Int16 PCM.
    All pipelines accept 16kHz PCM16 natively.
    """
    await browser.accept()

    aai_key     = await _optional_key(browser, get_assemblyai_key, "aai")
    mistral_key = await _optional_key(browser, get_mistral_key, "voxtral")
    soniox_key  = await _optional_key(browser, get_soniox_key, "soniox")
    gladia_key  = await _optional_key(browser, get_gladia_key, "gladia")

    aai     = AssemblyAIPipeline(lang, aai_key)     if aai_key     else None
    voxtral = VoxtralPipeline(lang, mistral_key)    if mistral_key else None
    soniox  = SonioxPipeline(lang, soniox_key)      if soniox_key  else None
    gladia  = GladiaPipeline(lang, gladia_key)      if gladia_key  else None

    connect_targets = []
    if aai:     connect_targets.append((aai,     "AAI U3",  "aai"))
    if voxtral: connect_targets.append((voxtral, "Voxtral", "voxtral"))
    if soniox:  connect_targets.append((soniox,  "Soniox",  "soniox"))
    if gladia:  connect_targets.append((gladia,  "Gladia",  "gladia"))

    for pipeline, label, src in connect_targets:
        try:
            await pipeline.connect()
        except Exception as e:
            print("[" + label + "] FAILED: " + str(e))
            try:
                await browser.send_json({"source": src, "error": str(e)})
            except Exception:
                pass

    if not any(p.ws for p, _, _ in connect_targets):
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
        try:
            while True:
                data = await browser.receive_bytes()
                relay_tasks = [p.send_audio(data) for p, _, _ in connect_targets if p.ws]
                if relay_tasks:
                    await asyncio.gather(*relay_tasks, return_exceptions=True)
        except Exception:
            pass

    tasks = [asyncio.create_task(audio_relay())]
    for pipeline, _, _ in connect_targets:
        tasks.append(asyncio.create_task(pipeline.receive_loop(send_to_browser)))

    try:
        await asyncio.gather(*tasks)
    except Exception:
        pass
    finally:
        for t in tasks:
            t.cancel()
        for pipeline, _, _ in connect_targets:
            await pipeline.close()
        try:
            await browser.close()
        except Exception:
            pass
