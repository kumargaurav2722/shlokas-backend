from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Optional

from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.pagesizes import letter


def _find_devanagari_font() -> Optional[Path]:
    candidates = [
        Path("/System/Library/Fonts/Supplemental/Devanagari Sangam MN.ttf"),
        Path("/System/Library/Fonts/Supplemental/Kohinoor Devanagari.ttf"),
        Path("/Library/Fonts/NotoSansDevanagari-Regular.ttf"),
        Path("/Library/Fonts/NotoSansDevanagariUI-Regular.ttf"),
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def generate_pdf_bytes(text: str, title: str = "Document") -> bytes:
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    font_name = "Helvetica"
    font_path = _find_devanagari_font()
    if font_path:
        font_name = "Devanagari"
        pdfmetrics.registerFont(TTFont(font_name, str(font_path)))

    pdf.setTitle(title)
    pdf.setFont(font_name, 14)
    pdf.drawString(40, height - 50, title)

    pdf.setFont(font_name, 11)
    text_object = pdf.beginText(40, height - 80)
    text_object.setLeading(16)

    for line in text.splitlines():
        if text_object.getY() < 60:
            pdf.drawText(text_object)
            pdf.showPage()
            pdf.setFont(font_name, 11)
            text_object = pdf.beginText(40, height - 60)
            text_object.setLeading(16)
        text_object.textLine(line)

    pdf.drawText(text_object)
    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    return buffer.read()
