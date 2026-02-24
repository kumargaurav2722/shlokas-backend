from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse, HTMLResponse, FileResponse, Response
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.chalisa import Chalisa
from app.models.puja_vidhi import PujaVidhi
from app.utils.pdf import generate_pdf_bytes

router = APIRouter(prefix="/devotion", tags=["Devotion"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _file_url(path: str | None) -> str:
    if not path:
        return ""
    normalized = path.replace("\\", "/")
    if normalized.startswith("data/"):
        return "/assets/" + normalized[len("data/"):]
    if normalized.startswith("/"):
        return normalized
    return "/assets/" + normalized


def _snippet(text: str | None, limit: int = 160) -> str:
    if not text:
        return ""
    compact = " ".join(text.split())
    return compact[:limit] + ("…" if len(compact) > limit else "")


def _render_html(title: str, body: str) -> HTMLResponse:
    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{
      color-scheme: light;
    }}
    body {{
      margin: 0;
      font-family: \"Iowan Old Style\", \"Times New Roman\", serif;
      background: #f8f4ee;
      color: #2b2116;
    }}
    .wrap {{
      max-width: 820px;
      margin: 40px auto;
      background: #fffaf3;
      border: 1px solid #f0e3d0;
      border-radius: 16px;
      padding: 32px;
      box-shadow: 0 16px 40px rgba(83, 63, 40, 0.08);
    }}
    h1 {{
      margin: 0 0 16px;
      font-size: 28px;
      color: #7a3d00;
    }}
    .content {{
      white-space: pre-wrap;
      line-height: 1.7;
      font-size: 16px;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>{title}</h1>
    <div class="content">{body}</div>
  </div>
</body>
</html>
"""
    return HTMLResponse(html)


@router.get("/chalisas")
def list_chalisas(
    query: str | None = None,
    deity: str | None = None,
    type: str | None = None,
    language: str | None = None,
    db: Session = Depends(get_db),
):
    if type and type.lower() not in {"chalisa", "chalisas"}:
        return []
    q = db.query(Chalisa)
    if deity:
        q = q.filter(Chalisa.deity == deity)
    if language:
        q = q.filter(Chalisa.language == language)
    if query:
        like = f"%{query}%"
        q = q.filter(
            or_(
                Chalisa.title.ilike(like),
                Chalisa.deity.ilike(like),
                Chalisa.content.ilike(like),
            )
        )
    rows = q.all()
    return [
        {
            "id": r.id,
            "title": r.title,
            "description": _snippet(r.content),
            "deity": r.deity,
            "type": "Chalisa",
            "tags": [r.deity.lower()] if r.deity else [],
            "text_url": f"/devotion/chalisas/{r.id}/view"
            if r.content_type == "text"
            else "",
            "pdf_url": _file_url(r.file_path)
            if r.content_type == "pdf"
            else f"/devotion/chalisas/{r.id}/pdf"
        }
        for r in rows
    ]


@router.get("/chalisas/{chalisa_id}")
def get_chalisa(chalisa_id: str, db: Session = Depends(get_db)):
    record = db.query(Chalisa).filter(Chalisa.id == chalisa_id).first()
    if not record:
        raise HTTPException(404, "Chalisa not found")
    return record


@router.get("/chalisas/{chalisa_id}/text")
def get_chalisa_text(chalisa_id: str, db: Session = Depends(get_db)):
    record = db.query(Chalisa).filter(Chalisa.id == chalisa_id).first()
    if not record:
        raise HTTPException(404, "Chalisa not found")
    if record.content_type != "text" or not record.content:
        raise HTTPException(404, "Chalisa text not available")
    return PlainTextResponse(record.content)


@router.get("/chalisas/{chalisa_id}/view")
def get_chalisa_view(chalisa_id: str, db: Session = Depends(get_db)):
    record = db.query(Chalisa).filter(Chalisa.id == chalisa_id).first()
    if not record:
        raise HTTPException(404, "Chalisa not found")
    if record.content_type != "text" or not record.content:
        raise HTTPException(404, "Chalisa text not available")
    return _render_html(record.title, record.content)


@router.get("/chalisas/{chalisa_id}/pdf")
def get_chalisa_pdf(chalisa_id: str, db: Session = Depends(get_db)):
    record = db.query(Chalisa).filter(Chalisa.id == chalisa_id).first()
    if not record:
        raise HTTPException(404, "Chalisa not found")
    if record.content_type == "pdf" and record.file_path:
        return FileResponse(record.file_path, media_type="application/pdf")
    if record.content_type != "text" or not record.content:
        raise HTTPException(404, "Chalisa text not available")
    pdf_bytes = generate_pdf_bytes(record.content, record.title)
    headers = {"Content-Disposition": f"attachment; filename=\"{record.title}.pdf\""}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)


@router.get("/puja-vidhi")
def list_puja_vidhi(
    query: str | None = None,
    deity: str | None = None,
    type: str | None = None,
    language: str | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(PujaVidhi)
    if deity:
        q = q.filter(PujaVidhi.deity == deity)
    if language:
        q = q.filter(PujaVidhi.language == language)
    if query:
        like = f"%{query}%"
        q = q.filter(
            or_(
                PujaVidhi.title.ilike(like),
                PujaVidhi.deity.ilike(like),
                PujaVidhi.content.ilike(like),
            )
        )
    rows = q.all()
    label = type or "Puja"
    return [
        {
            "id": r.id,
            "title": r.title,
            "description": _snippet(r.content),
            "deity": r.deity,
            "type": label,
            "tags": [r.deity.lower()] if r.deity else [],
            "text_url": f"/devotion/puja-vidhi/{r.id}/view"
            if r.content_type == "text"
            else "",
            "pdf_url": _file_url(r.file_path)
            if r.content_type == "pdf"
            else f"/devotion/puja-vidhi/{r.id}/pdf"
        }
        for r in rows
    ]


@router.get("/puja-vidhi/{vidhi_id}")
def get_puja_vidhi(vidhi_id: str, db: Session = Depends(get_db)):
    record = db.query(PujaVidhi).filter(PujaVidhi.id == vidhi_id).first()
    if not record:
        raise HTTPException(404, "Puja vidhi not found")
    return record


@router.get("/puja-vidhi/{vidhi_id}/text")
def get_puja_vidhi_text(vidhi_id: str, db: Session = Depends(get_db)):
    record = db.query(PujaVidhi).filter(PujaVidhi.id == vidhi_id).first()
    if not record:
        raise HTTPException(404, "Puja vidhi not found")
    if record.content_type != "text" or not record.content:
        raise HTTPException(404, "Puja vidhi text not available")
    return PlainTextResponse(record.content)


@router.get("/puja-vidhi/{vidhi_id}/view")
def get_puja_vidhi_view(vidhi_id: str, db: Session = Depends(get_db)):
    record = db.query(PujaVidhi).filter(PujaVidhi.id == vidhi_id).first()
    if not record:
        raise HTTPException(404, "Puja vidhi not found")
    if record.content_type != "text" or not record.content:
        raise HTTPException(404, "Puja vidhi text not available")
    return _render_html(record.title, record.content)


@router.get("/puja-vidhi/{vidhi_id}/pdf")
def get_puja_vidhi_pdf(vidhi_id: str, db: Session = Depends(get_db)):
    record = db.query(PujaVidhi).filter(PujaVidhi.id == vidhi_id).first()
    if not record:
        raise HTTPException(404, "Puja vidhi not found")
    if record.content_type == "pdf" and record.file_path:
        return FileResponse(record.file_path, media_type="application/pdf")
    if record.content_type != "text" or not record.content:
        raise HTTPException(404, "Puja vidhi text not available")
    pdf_bytes = generate_pdf_bytes(record.content, record.title)
    headers = {"Content-Disposition": f"attachment; filename=\"{record.title}.pdf\""}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)
