import argparse
import uuid
from collections import defaultdict

from sqlalchemy import delete

from app.database import SessionLocal, Base, engine
from app.models.text import Text
from app.models.translation import Translation
from app.models.topic import Topic, TopicItem, TopicTranslation

TOPIC_DEFINITIONS = {
    "karma": {
        "name": "Karma",
        "description": "Duty, action, and right effort.",
        "keywords": [
            "karma",
            "action",
            "duty",
            "work",
            "कर्म",
            "कर्तव्य",
            "कर्मयोग",
            "फल",
            "फलाशा",
        ],
    },
    "bhakti": {
        "name": "Bhakti",
        "description": "Devotion, love, and surrender to the divine.",
        "keywords": [
            "bhakti",
            "devotion",
            "surrender",
            "love",
            "worship",
            "भक्ति",
            "श्रद्धा",
            "आराधना",
            "ईश्वर",
            "भगवान",
        ],
    },
    "meditation": {
        "name": "Meditation",
        "description": "Dhyana, inner stillness, and focus.",
        "keywords": [
            "meditation",
            "meditate",
            "dhyana",
            "dhyan",
            "yoga",
            "ध्यान",
            "ध्यानयोग",
            "समाधि",
            "चित्त",
            "मन",
        ],
    },
    "fear": {
        "name": "Fear",
        "description": "Fearlessness and courage in adversity.",
        "keywords": [
            "fear",
            "fearless",
            "afraid",
            "bhaya",
            "भय",
            "निडर",
            "भयभीत",
            "डर",
        ],
    },
    "anxiety": {
        "name": "Anxiety",
        "description": "Calm, steadiness, and relief from worry.",
        "keywords": [
            "anxiety",
            "anxious",
            "worry",
            "stress",
            "चिंता",
            "बेचैनी",
            "व्याकुल",
            "तनाव",
        ],
    },
    "anger": {
        "name": "Anger",
        "description": "Mastery over anger and reactive impulses.",
        "keywords": [
            "anger",
            "angry",
            "wrath",
            "क्रोध",
            "रोष",
            "कुपित",
        ],
    },
    "detachment": {
        "name": "Detachment",
        "description": "Non-attachment, renunciation, and balance.",
        "keywords": [
            "detachment",
            "non-attachment",
            "renunciation",
            "renounce",
            "वैराग्य",
            "अनासक्ति",
            "त्याग",
        ],
    },
    "success": {
        "name": "Success",
        "description": "Victory, achievement, and fulfillment.",
        "keywords": [
            "success",
            "achieve",
            "victory",
            "succeed",
            "सफल",
            "सफलता",
            "विजय",
            "लाभ",
        ],
    },
    "failure": {
        "name": "Failure",
        "description": "Resilience through defeat and setbacks.",
        "keywords": [
            "failure",
            "defeat",
            "lose",
            "lost",
            "असफल",
            "हार",
            "विफल",
        ],
    },
    "death": {
        "name": "Death",
        "description": "Mortality, impermanence, and the eternal self.",
        "keywords": [
            "death",
            "die",
            "mortal",
            "mortality",
            "मृत्यु",
            "मरना",
            "नाश",
            "अमर",
        ],
    },
    "self-realization": {
        "name": "Self-Realization",
        "description": "Knowing the self and the eternal soul.",
        "keywords": [
            "self-realization",
            "self knowledge",
            "self",
            "atma",
            "ātman",
            "आत्म",
            "आत्मा",
            "स्व",
            "स्वरूप",
            "ब्रह्म",
        ],
    },
    "devotion": {
        "name": "Devotion",
        "description": "Deep love and reverence for the divine.",
        "keywords": [
            "devotion",
            "devotee",
            "surrender",
            "भक्ति",
            "श्रद्धा",
            "पूजा",
            "आराधना",
        ],
    },
}

TOPIC_TRANSLATIONS = {
    "hi": {
        "karma": ("कर्म", "कर्तव्य, कार्य और सही प्रयास।"),
        "bhakti": ("भक्ति", "ईश्वर के प्रति प्रेम, श्रद्धा और समर्पण।"),
        "meditation": ("ध्यान", "अंतर्मन की स्थिरता और एकाग्रता।"),
        "fear": ("भय", "निर्भयता और साहस।"),
        "anxiety": ("चिंता", "शांति, स्थिरता और बेचैनी से राहत।"),
        "anger": ("क्रोध", "क्रोध पर नियंत्रण और संयम।"),
        "detachment": ("वैराग्य", "अनासक्ति, त्याग और संतुलन।"),
        "success": ("सफलता", "उपलब्धि, विजय और पूर्ति।"),
        "failure": ("विफलता", "पराजय में धैर्य और पुनः उठने की क्षमता।"),
        "death": ("मृत्यु", "नश्वरता और शाश्वत आत्मा का बोध।"),
        "self-realization": ("आत्म-बोध", "आत्मा और सत्य का ज्ञान।"),
        "devotion": ("समर्पण", "गहन प्रेम और श्रद्धा।"),
    },
    "bn": {
        "karma": ("কর্ম", "কর্তব্য, কর্ম এবং সঠিক প্রচেষ্টা।"),
        "bhakti": ("ভক্তি", "ভগবানের প্রতি প্রেম, শ্রদ্ধা ও সমর্পণ।"),
        "meditation": ("ধ্যান", "মনের স্থিরতা ও একাগ্রতা।"),
        "fear": ("ভয়", "নির্ভীকতা ও সাহস।"),
        "anxiety": ("উদ্বেগ", "শান্তি, স্থিরতা ও উদ্বেগ থেকে মুক্তি।"),
        "anger": ("ক্রোধ", "ক্রোধ নিয়ন্ত্রণ ও সংযম।"),
        "detachment": ("বৈরাগ্য", "অাসক্তিহীনতা, ত্যাগ ও ভারসাম্য।"),
        "success": ("সাফল্য", "অর্জন, বিজয় ও পূর্ণতা।"),
        "failure": ("ব্যর্থতা", "পরাজয়ের পর ধৈর্য ও ঘুরে দাঁড়ানো।"),
        "death": ("মৃত্যু", "নশ্বরতা ও চিরন্তন আত্মার উপলব্ধি।"),
        "self-realization": ("আত্ম-উপলব্ধি", "স্ব ও চিরন্তনের জ্ঞান।"),
        "devotion": ("সমর্পণ", "গভীর ভক্তি ও শ্রদ্ধা।"),
    },
    "mr": {
        "karma": ("कर्म", "कर्तव्य, कृती आणि योग्य प्रयत्न."),
        "bhakti": ("भक्ती", "ईश्वरप्रेम, श्रद्धा आणि समर्पण."),
        "meditation": ("ध्यान", "मनाची स्थिरता आणि एकाग्रता."),
        "fear": ("भीती", "निर्भयता आणि धैर्य."),
        "anxiety": ("चिंता", "शांती, स्थैर्य आणि चिंता कमी करणे."),
        "anger": ("क्रोध", "क्रोधावर नियंत्रण आणि संयम."),
        "detachment": ("वैऱाग्य", "अनासक्ती, त्याग आणि संतुलन."),
        "success": ("यश", "यश, विजय आणि समाधान."),
        "failure": ("अपयश", "पराभवानंतर संयम आणि पुन्हा उभे राहणे."),
        "death": ("मृत्यू", "नश्वरता आणि चिरंतन आत्म्याची जाणीव."),
        "self-realization": ("आत्मबोध", "स्वतःचे आणि आत्म्याचे ज्ञान."),
        "devotion": ("समर्पण", "गहन भक्ती आणि आदर."),
    },
    "te": {
        "karma": ("కర్మ", "कर्तవ్యం, పని మరియు సరైన ప్రయత్నం."),
        "bhakti": ("భక్తి", "దైవ ప్రేమ, శ్రద్ధ మరియు సమర్పణ."),
        "meditation": ("ధ్యానం", "మనస్సు స్థిరత్వం మరియు ఏకాగ్రత."),
        "fear": ("భయం", "నిర్భయత మరియు ధైర్యం."),
        "anxiety": ("ఆందోళన", "శాంతి, స్థిరత్వం మరియు ఆందోళన తగ్గింపు."),
        "anger": ("క్రోధం", "క్రోధ నియంత్రణ మరియు నియమం."),
        "detachment": ("వైరాగ్యం", "అనాసక్తి, త్యాగం మరియు సమతుల్యత."),
        "success": ("విజయం", "సాధన, విజయము మరియు సంతృప్తి."),
        "failure": ("విఫలం", "పరాజయం తర్వాత ధైర్యం మరియు పునరుద్ధరణ."),
        "death": ("మరణం", "నశ్వరత మరియు శాశ్వత ఆత్మ యొక్క బోధ."),
        "self-realization": ("ఆత్మజ్ఞానం", "ఆత్మ మరియు సత్యం పై అవగాహన."),
        "devotion": ("సమర్పణ", "గాఢమైన భక్తి మరియు గౌరవం."),
    },
    "ta": {
        "karma": ("கர்மம்", "கடமை, செயல் மற்றும் சரியான முயற்சி."),
        "bhakti": ("பக்தி", "இறை அன்பு, நம்பிக்கை மற்றும் சரணடைவு."),
        "meditation": ("தியானம்", "மன அமைதி மற்றும் ஒருமுகத்தன்மை."),
        "fear": ("பயம்", "அச்சமின்மை மற்றும் துணிவு."),
        "anxiety": ("கவலை", "அமைதி, நிலைத்தன்மை மற்றும் கவலைக் குறைப்பு."),
        "anger": ("கோபம்", "கோப கட்டுப்பாடு மற்றும் संयமம்."),
        "detachment": ("துறவு", "இணைப்பின்மை, தியாகம் மற்றும் சமநிலை."),
        "success": ("வெற்றி", "சாதனை, வெற்றி மற்றும் நிறைவு."),
        "failure": ("தோல்வி", "தோல்விக்குப் பின் தன்னம்பிக்கை மற்றும் மீளுதல்."),
        "death": ("மரணம்", "நశ్వరता மற்றும் நித்திய ஆன்மாவின் உணர்வு."),
        "self-realization": ("ஆத்ம விழிப்பு", "ஆத்மாவையும் சத்தியத்தையும் அறிதல்."),
        "devotion": ("சரண்", "ஆழ்ந்த பக்தி மற்றும் மரியாதை."),
    },
    "kn": {
        "karma": ("ಕರ್ಮ", "ಕರ್ತವ್ಯ, ಕಾರ್ಯ ಮತ್ತು ಸರಿಯಾದ ಪ್ರಯತ್ನ."),
        "bhakti": ("ಭಕ್ತಿ", "ದೈವ ಪ್ರೀತಿ, ಶ್ರದ್ಧೆ ಮತ್ತು ಸಮರ್ಪಣೆ."),
        "meditation": ("ಧ್ಯಾನ", "ಮನಸ್ಸಿನ ಸ್ಥಿರತೆ ಮತ್ತು ಏಕಾಗ್ರತೆ."),
        "fear": ("ಭಯ", "ನಿರ್ಭಯತೆ ಮತ್ತು ಧೈರ್ಯ."),
        "anxiety": ("ಆತಂಕ", "ಶಾಂತಿ, ಸ್ಥಿರತೆ ಮತ್ತು ಆತಂಕ ನಿವಾರಣೆ."),
        "anger": ("ಕ್ರೋಧ", "ಕ್ರೋಧ ನಿಯಂತ್ರಣ ಮತ್ತು ಸಂಯಮ."),
        "detachment": ("ವೈರಾಗ್ಯ", "ಅನಾಸಕ್ತಿ, ತ್ಯಾಗ ಮತ್ತು ಸಮತೋಲನ."),
        "success": ("ಯಶಸ್ಸು", "ಸಾಧನೆ, ಜಯ ಮತ್ತು ತೃಪ್ತಿ."),
        "failure": ("ವಿಫಲತೆ", "ಪರಾಭವದ ನಂತರ ಧೈರ್ಯ ಮತ್ತು ಪುನರುತ್ಥಾನ."),
        "death": ("ಮರಣ", "ನಶ್ವರತೆ ಮತ್ತು ಶಾಶ್ವತ ಆತ್ಮದ ಅರಿವು."),
        "self-realization": ("ಆತ್ಮಬೋಧ", "ಸ್ವ ಮತ್ತು ಆತ್ಮಜ್ಞಾನ."),
        "devotion": ("ಸಮರ್ಪಣೆ", "ಆಳವಾದ ಭಕ್ತಿ ಮತ್ತು ಗೌರವ."),
    },
}


def build_topics(limit: int | None = None, clear: bool = True):
    engine.echo = False
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        topic_rows = {}
        for slug, info in TOPIC_DEFINITIONS.items():
            row = db.query(Topic).filter(Topic.slug == slug).first()
            if not row:
                row = Topic(
                    slug=slug,
                    name=info["name"],
                    description=info["description"],
                )
                db.add(row)
            else:
                row.name = info["name"]
                row.description = info["description"]
            topic_rows[slug] = row
        db.commit()

        if clear:
            db.execute(delete(TopicItem))
            db.execute(delete(TopicTranslation))
            db.commit()

        translations_batch = []
        for lang, mapping in TOPIC_TRANSLATIONS.items():
            for slug, (name, description) in mapping.items():
                topic_id = topic_rows[slug].id
                translations_batch.append(
                    TopicTranslation(
                        topic_id=topic_id,
                        language=lang,
                        name=name,
                        description=description,
                    )
                )
        if translations_batch:
            db.bulk_save_objects(translations_batch)
            db.commit()

        texts_query = db.query(Text)
        if limit:
            texts_query = texts_query.limit(limit)
        texts = texts_query.all()

        translations = (
            db.query(Translation)
            .filter(Translation.language.in_(["English", "Hindi"]))
            .all()
        )
        tr_by_id = defaultdict(dict)
        for tr in translations:
            tr_by_id[tr.text_id][tr.language] = tr.translation

        keyword_map = {
            slug: [kw.lower() for kw in info["keywords"]]
            for slug, info in TOPIC_DEFINITIONS.items()
        }

        batch = []
        counts = defaultdict(int)

        for text in texts:
            tr_map = tr_by_id.get(text.id, {})
            combined = " ".join(
                filter(
                    None,
                    [
                        text.sanskrit or "",
                        tr_map.get("English", ""),
                        tr_map.get("Hindi", ""),
                    ],
                )
            ).lower()
            if not combined.strip():
                continue

            for slug, keywords in keyword_map.items():
                matches = [kw for kw in keywords if kw in combined]
                if not matches:
                    continue
                try:
                    text_uuid = uuid.UUID(str(text.id))
                except ValueError:
                    text_uuid = None
                batch.append(
                    TopicItem(
                        topic_id=topic_rows[slug].id,
                        text_id=text_uuid or text.id,
                        score=len(matches),
                        matched_keyword=matches[0],
                    )
                )
                counts[slug] += 1

            if len(batch) >= 1000:
                db.bulk_save_objects(batch)
                db.commit()
                batch.clear()

        if batch:
            db.bulk_save_objects(batch)
            db.commit()

        print("Topic generation complete:")
        for slug, count in sorted(counts.items()):
            print(f"  {slug}: {count} verses")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build topic mappings from translations.")
    parser.add_argument("--limit", type=int, default=None, help="Limit verses for testing.")
    parser.add_argument("--no-clear", action="store_true", help="Do not clear existing topic items.")
    args = parser.parse_args()

    build_topics(limit=args.limit, clear=not args.no_clear)
