import os
import platform
import shutil
import subprocess
from typing import Optional, Tuple

BASE_AUDIO_PATH = "audio"

_tts_model = None


def _get_tts_model():
    global _tts_model
    if _tts_model is None:
        from TTS.api import TTS
        _tts_model = TTS(
            model_name="tts_models/multilingual/multi-dataset/xtts_v2",
            progress_bar=False,
            gpu=False
        )
    return _tts_model


def _detect_engine() -> str:
    forced = os.getenv("TTS_ENGINE")
    if forced:
        return forced.lower()

    if platform.system() == "Darwin" and shutil.which("say"):
        return "say"

    if shutil.which("piper"):
        return "piper"

    return "xtts"


def _say_tts(text: str, out_base: str) -> Tuple[str, str]:
    aiff_path = out_base + ".aiff"
    m4a_path = out_base + ".m4a"
    txt_path = out_base + ".txt"

    with open(txt_path, "w", encoding="utf-8") as handle:
        handle.write(text)

    say_cmd = ["say", "-o", aiff_path, "-f", txt_path]
    subprocess.run(say_cmd, check=True)

    if shutil.which("afconvert"):
        convert_cmd = [
            "afconvert",
            aiff_path,
            m4a_path,
            "-f",
            "m4af",
            "-d",
            "aac",
        ]
        subprocess.run(convert_cmd, check=True)
        os.remove(aiff_path)
        os.remove(txt_path)
        return m4a_path, "say"

    os.remove(txt_path)
    return aiff_path, "say"


def _piper_tts(text: str, out_base: str) -> Tuple[str, str]:
    model_path = os.getenv("PIPER_MODEL")
    if not model_path:
        raise RuntimeError("PIPER_MODEL is not set")
    wav_path = out_base + ".wav"
    cmd = ["piper", "--model", model_path, "--output_file", wav_path]
    subprocess.run(cmd, input=text.encode("utf-8"), check=True)
    return wav_path, "piper"


def _xtts_tts(text: str, out_base: str) -> Tuple[str, str]:
    mp3_path = out_base + ".mp3"
    _get_tts_model().tts_to_file(text=text, file_path=mp3_path)
    return mp3_path, "xtts"


def generate_audio(text: str, text_id: str, language: str) -> Tuple[str, str]:
    text_id = str(text_id)
    folder = os.path.join(BASE_AUDIO_PATH, text_id)
    os.makedirs(folder, exist_ok=True)

    out_base = os.path.join(folder, language)
    engine = _detect_engine()

    if engine == "say":
        for ext in (".m4a", ".aiff"):
            existing = out_base + ext
            if os.path.exists(existing):
                return existing, "say"
        return _say_tts(text, out_base)

    if engine == "piper":
        existing = out_base + ".wav"
        if os.path.exists(existing):
            return existing, "piper"
        return _piper_tts(text, out_base)

    existing = out_base + ".mp3"
    if os.path.exists(existing):
        return existing, "xtts"
    return _xtts_tts(text, out_base)
