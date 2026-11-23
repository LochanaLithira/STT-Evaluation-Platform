"""API key loading from environment / .env."""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass


def get_assemblyai_key():
    key = os.environ.get("ASSEMBLYAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "ASSEMBLYAI_API_KEY not set. Add it to the .env file next to app.py"
        )
    return key
