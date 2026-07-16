"""
STT Comparison Test App - Nova-3 vs Flux vs AssemblyAI U3 Pro vs OpenAI vs Voxtral
====================================================================================
Set your API keys in .env:
    DEEPGRAM_API_KEY=your_deepgram_key
    ASSEMBLYAI_API_KEY=your_assemblyai_key
    OPENAI_API_KEY=your_openai_key
    MISTRAL_API_KEY=your_mistral_key

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

from config import get_api_key, get_assemblyai_key, get_openai_key, get_mistral_key
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
    print("  STT 5-Way Comparison")
    print("  Nova-3 | Flux | AssemblyAI U3 Pro | OpenAI | Voxtral")
    print("-" * 60)
    try:
        k = get_api_key()
        print("  Deepgram key:    " + k[:8] + "*" * 12)
    except RuntimeError as e:
        print("  WARNING: " + str(e))
    try:
        k2 = get_assemblyai_key()
        print("  AssemblyAI key:  " + k2[:8] + "*" * 12)
    except RuntimeError as e:
        print("  WARNING: " + str(e))
    try:
        k3 = get_openai_key()
        print("  OpenAI key:      " + k3[:8] + "*" * 12)
    except RuntimeError as e:
        print("  WARNING: " + str(e))
    try:
        k4 = get_mistral_key()
        print("  Mistral key:     " + k4[:8] + "*" * 12)
    except RuntimeError as e:
        print("  WARNING: " + str(e))
    print("  Open: http://localhost:8765")
    print("=" * 60 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8765)
