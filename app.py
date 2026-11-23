"""
AssemblyAI Realtime Model Comparison
====================================
Streams your mic to two AssemblyAI realtime models side by side:
    Universal-3.5 Pro Realtime   (universal-3-5-pro)
    STT-3.7 Realtime Agent       (assemblyai-stt-3-7-realtime-agent)

Set your API key in .env:
    ASSEMBLYAI_API_KEY=your_assemblyai_key

Run:
    pip install -r requirements.txt
    python app.py

Open: http://localhost:8765
"""
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from config import get_assemblyai_key
from routes.compare import router as compare_router

HERE = Path(__file__).parent

app = FastAPI()
app.include_router(compare_router)
app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")


@app.get("/")
async def get():
    return HTMLResponse((HERE / "static" / "index.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  AssemblyAI Realtime Comparison")
    print("  Universal-3.5 Pro | STT-3.7 Realtime Agent")
    print("-" * 60)
    try:
        k = get_assemblyai_key()
        print("  AssemblyAI key:  " + k[:8] + "*" * 12)
    except RuntimeError as e:
        print("  WARNING: " + str(e))
    print("  Open: http://localhost:8765")
    print("=" * 60 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8765)
