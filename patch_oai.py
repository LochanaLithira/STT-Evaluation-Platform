with open('test_app.py', encoding='utf-8') as f:
    lines = f.readlines()

# Find and replace the session.update block
start = None
end = None
for i, line in enumerate(lines):
    if 'audio.input nested format WITH type=transcription' in line and start is None:
        start = i
    if start is not None and 'audio.input nested + type=transcription' in line:
        end = i + 1
        break

print(f"Found block at lines {start+1}-{end}")

new_block = '''\
        # CONFIRMED valid session.update shape for gpt-realtime-whisper:
        # - type="transcription" is REQUIRED
        # - audio.input.format and audio.input.transcription are accepted
        # - audio.input.turn_detection is NOT supported (model has built-in VAD)
        await self.ws.send(json.dumps({
            "type": "session.update",
            "session": {
                "type": "transcription",
                "audio": {
                    "input": {
                        "format": {"type": "audio/pcm", "rate": 24000},
                        "transcription": transcription_cfg,
                        # NOTE: turn_detection OMITTED - not supported by gpt-realtime-whisper
                        # The model uses built-in VAD (speech_started/stopped fire automatically)
                    }
                },
            }
        }))
        print("[OpenAI] session.update sent - no turn_detection (built-in VAD)", flush=True)
'''

lines[start:end] = [new_block]

with open('test_app.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)
print('PATCHED OK, total lines:', len(lines))
