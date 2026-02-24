"""
Seed Upanishads, Puranas, and fix Ramcharitmanas data.
Provides essential mantras from major Upanishads and key Purana verses
so the UI has content to display.

Usage:
    python3 -m app.scripts.seed_extras --dry-run
    python3 -m app.scripts.seed_extras
"""

import argparse
import json
import os
import re
import sys
from typing import Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.database import SessionLocal
from app.models.text import Text

DATA_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "dharmicdata",
)

# ---------------------------------------------------------------------------
# Upanishad seed data (10 principal Upanishads)
# ---------------------------------------------------------------------------

UPANISHAD_MANTRAS = {
    "Isha Upanishad": [
        (1, 1, "ॐ ईशा वास्यमिदं सर्वं यत्किञ्च जगत्यां जगत् । तेन त्यक्तेन भुञ्जीथा मा गृधः कस्यस्विद्धनम् ॥"),
        (1, 2, "कुर्वन्नेवेह कर्माणि जिजीविषेच्छतं समाः । एवं त्वयि नान्यथेतोऽस्ति न कर्म लिप्यते नरे ॥"),
        (1, 3, "असुर्या नाम ते लोका अन्धेन तमसावृताः । ताँस्ते प्रेत्याभिगच्छन्ति ये के चात्महनो जनाः ॥"),
        (1, 4, "अनेजदेकं मनसो जवीयो नैनद्देवा आप्नुवन्पूर्वमर्षत् । तद्धावतोऽन्यानत्येति तिष्ठत्तस्मिन्नपो मातरिश्वा दधाति ॥"),
        (1, 5, "तदेजति तन्नैजति तद्दूरे तद्वन्तिके । तदन्तरस्य सर्वस्य तदु सर्वस्यास्य बाह्यतः ॥"),
        (1, 6, "यस्तु सर्वाणि भूतानि आत्मन्येवानुपश्यति । सर्वभूतेषु चात्मानं ततो न विजुगुप्सते ॥"),
        (1, 7, "यस्मिन्सर्वाणि भूतानि आत्मैवाभूद्विजानतः । तत्र को मोहः कः शोक एकत्वमनुपश्यतः ॥"),
        (1, 8, "स पर्यगाच्छुक्रमकायमव्रणमस्नाविरँ शुद्धमपापविद्धम् । कविर्मनीषी परिभूःस्वयम्भूर्याथातथ्यतोऽर्थान् व्यदधाच्छाश्वतीभ्यः समाभ्यः ॥"),
    ],
    "Kena Upanishad": [
        (1, 1, "केनेषितं पतति प्रेषितं मनः केन प्राणः प्रथमः प्रैति युक्तः । केनेषितां वाचमिमां वदन्ति चक्षुः श्रोत्रं क उ देवो युनक्ति ॥"),
        (1, 2, "श्रोत्रस्य श्रोत्रं मनसो मनो यद्वाचो ह वाचं स उ प्राणस्य प्राणः । चक्षुषश्चक्षुरतिमुच्य धीराः प्रेत्यास्माल्लोकादमृता भवन्ति ॥"),
        (1, 3, "न तत्र चक्षुर्गच्छति न वाग्गच्छति नो मनो न विद्मो न विजानीमो यथैतदनुशिष्यात् ॥"),
        (1, 4, "अन्यदेव तद्विदितादथो अविदितादधि इति शुश्रुम पूर्वेषां ये नस्तद्व्याचचक्षिरे ॥"),
    ],
    "Katha Upanishad": [
        (1, 1, "उशन् ह वै वाजश्रवसः सर्ववेदसं ददौ । तस्य ह नचिकेता नाम पुत्र आस ॥"),
        (1, 2, "तं ह कुमारं सन्तं दक्षिणासु नीयमानासु श्रद्धाविवेश सोऽमन्यत ॥"),
        (1, 3, "पीतोदका जग्धतृणा दुग्धदोहा निरिन्द्रियाः अनन्दा नाम ते लोकास्तान् स गच्छति ता ददत् ॥"),
        (2, 1, "श्रेयश्च प्रेयश्च मनुष्यमेतस्तौ सम्परीत्य विविनक्ति धीरः । श्रेयो हि धीरोऽभि प्रेयसो वृणीते प्रेयो मन्दो योगक्षेमाद्वृणीते ॥"),
        (2, 2, "उत्तिष्ठत जाग्रत प्राप्य वरान्निबोधत । क्षुरस्य धारा निशिता दुरत्यया दुर्गं पथस्तत्कवयो वदन्ति ॥"),
    ],
    "Mundaka Upanishad": [
        (1, 1, "ब्रह्मा देवानां प्रथमः सम्बभूव विश्वस्य कर्ता भुवनस्य गोप्ता ।"),
        (1, 2, "द्वे विद्ये वेदितव्ये इति ह स्म यद्ब्रह्मविदो वदन्ति परा चैवापरा च ॥"),
        (2, 1, "तत्रापरा ऋग्वेदो यजुर्वेदः सामवेदोऽथर्ववेदः शिक्षा कल्पो व्याकरणं निरुक्तं छन्दो ज्योतिषमिति ।"),
        (3, 1, "सत्यमेव जयते नानृतं सत्येन पन्था विततो देवयानः ।"),
    ],
    "Mandukya Upanishad": [
        (1, 1, "ॐ इत्येतदक्षरमिदं सर्वं तस्योपव्याख्यानम् भूतं भवद्भविष्यदिति सर्वमोंकार एव ।"),
        (1, 2, "सर्वं ह्येतद्ब्रह्मायमात्मा ब्रह्म सोऽयमात्मा चतुष्पात् ।"),
        (1, 3, "जागरितस्थानो बहिःप्रज्ञः सप्ताङ्ग एकोनविंशतिमुखः स्थूलभुग्वैश्वानरः प्रथमः पादः ।"),
        (1, 4, "स्वप्नस्थानोऽन्तःप्रज्ञः सप्ताङ्ग एकोनविंशतिमुखः प्रविविक्तभुक्तैजसो द्वितीयः पादः ।"),
        (1, 5, "यत्र सुप्तो न कञ्चन कामं कामयते न कञ्चन स्वप्नं पश्यति तत्सुषुप्तम् ।"),
    ],
    "Taittiriya Upanishad": [
        (1, 1, "ॐ शं नो मित्रः शं वरुणः शं नो भवत्वर्यमा । शं न इन्द्रो बृहस्पतिः शं नो विष्णुरुरुक्रमः ।"),
        (1, 2, "ॐ शीक्षां व्याख्यास्यामः वर्णः स्वरः मात्रा बलम् साम सन्तानः ।"),
        (2, 1, "ब्रह्मविदाप्नोति परम् तदेषाभ्युक्ता सत्यं ज्ञानमनन्तं ब्रह्म ।"),
        (3, 1, "अन्नं ब्रह्मेति व्यजानात् अन्नाद्ध्येव खल्विमानि भूतानि जायन्ते ।"),
    ],
    "Chandogya Upanishad": [
        (1, 1, "ॐ इत्येतदक्षरमुद्गीथमुपासीतोम्इति ह्युद्गायति तस्योपव्याख्यानम् ।"),
        (3, 14, "सर्वं खल्विदं ब्रह्म तज्जलानिति शान्त उपासीत ।"),
        (6, 1, "सदेव सोम्येदमग्र आसीदेकमेवाद्वितीयम् ।"),
        (6, 2, "तत्त्वमसि श्वेतकेतो ।"),
        (6, 8, "स य एषोऽणिमैतदात्म्यमिदं सर्वं तत्सत्यं स आत्मा तत्त्वमसि श्वेतकेतो ।"),
    ],
    "Brihadaranyaka Upanishad": [
        (1, 3, "असतो मा सद्गमय तमसो मा ज्योतिर्गमय मृत्योर्मामृतं गमय ।"),
        (1, 4, "अहं ब्रह्मास्मि ।"),
        (2, 4, "आत्मनो वा अरे दर्शनेन श्रवणेन मत्या विज्ञानेनेदं सर्वं विदितम् ।"),
        (3, 9, "यो वै तद्ब्रह्म वेद यो वै तत्परमं ब्रह्म वेद ।"),
        (4, 4, "ब्रह्मैव सन्ब्रह्माप्येति ।"),
    ],
    "Prashna Upanishad": [
        (1, 1, "ॐ सुकेशा च भारद्वाजः शैब्यश्च सत्यकामः सौर्यायणी च गार्ग्यः कौसल्यश्चाश्वलायनो भार्गवो वैदर्भिः कबन्धी कात्यायनः ।"),
        (1, 2, "ते ह एते ब्रह्मपरा ब्रह्मनिष्ठाः परं ब्रह्मान्वेषमाणा एष ह वै तत्सर्वं वक्ष्यतीति ते ह समित्पाणयो भगवन्तं पिप्पलादमुपसन्नाः ।"),
        (2, 1, "अथ हैनं भार्गवो वैदर्भिः पप्रच्छ भगवन् कत्येव देवाः प्रजां विधारयन्ते कतर एतत्प्रकाशयन्ते कः पुनरेषां वरिष्ठ इति ।"),
        (3, 1, "अथ हैनं कौसल्यश्चाश्वलायनः पप्रच्छ भगवन्कुत एष प्राणो जायते कथमायात्यस्मिञ्शरीरे ।"),
    ],
    "Svetasvatara Upanishad": [
        (1, 1, "किं कारणं ब्रह्म कुतः स्म जाताः जीवाम केन क्व च सम्प्रतिष्ठाः ।"),
        (1, 3, "ते ध्यानयोगानुगता अपश्यन्देवात्मशक्तिं स्वगुणैर्निगूढाम् ।"),
        (3, 8, "वेदाहमेतं पुरुषं महान्तमादित्यवर्णं तमसः परस्तात् ।"),
        (4, 3, "त्वमस्त्री त्वं पुमानसि त्वं कुमारोत वा कुमारी ।"),
        (6, 11, "एको देवः सर्वभूतेषु गूढः सर्वव्यापी सर्वभूतान्तरात्मा ।"),
    ],
}

# ---------------------------------------------------------------------------
# Purana seed data (key verses from major Puranas)
# ---------------------------------------------------------------------------

PURANA_VERSES = {
    "Bhagavata Purana": [
        (1, 1, "जन्माद्यस्य यतोऽन्वयादितरतश्चार्थेष्वभिज्ञः स्वराट् ।"),
        (1, 2, "धर्मः प्रोज्झितकैतवोऽत्र परमो निर्मत्सराणां सतां वेद्यं वास्तवमत्र वस्तु ।"),
        (1, 3, "निगमकल्पतरोर्गलितं फलं शुकमुखादमृतद्रवसंयुतम् पिबत भागवतं रसमालयम् ।"),
        (2, 1, "एतन्निर्विद्यमानानामिच्छतामकुतोभयम् योगिनां नृप निर्णीतं हरेर्नामानुकीर्तनम् ॥"),
        (10, 1, "कृष्णद्वैपायनव्यासपूज्याभ्यामथ विष्णवे नमो गुरुभ्यो गोभ्यश्च ।"),
        (10, 14, "तत्तेऽनुकम्पां सुसमीक्षमाणो भुञ्जान एवात्मकृतं विपाकम् ।"),
        (10, 33, "विक्रीडितं व्रजवधूभिरिदं च विष्णोः श्रद्धान्वितोऽनुशृणुयादथ वर्णयेद्यः ।"),
    ],
    "Vishnu Purana": [
        (1, 1, "नमस्ते पुण्डरीकाक्ष नमस्ते पुरुषोत्तम ।  नमस्ते सर्वलोकेश नमस्ते तिग्मचक्रिण ॥"),
        (1, 2, "सृष्टिस्थितिविनाशानां शक्तिभूते सनातनि गुणाश्रये गुणमये नारायणि नमोऽस्तु ते ॥"),
        (1, 19, "विष्णुर्विश्वस्य कर्ता च भर्ता हर्ता च भगवान् ।"),
        (1, 22, "एष नारायणः साक्षाद्भगवानादिपूरुषः ।"),
    ],
    "Shiva Purana": [
        (1, 1, "नमः शिवाय शान्ताय कारणत्रयहेतवे ।"),
        (1, 5, "शिवो नित्यः शाश्वतो देवो विभुः सर्वहितो गतिः ।"),
        (2, 1, "शिवस्य परमं रूपं सच्चिदानन्दविग्रहम् ।"),
        (7, 1, "कर्पूरगौरं करुणावतारं संसारसारं भुजगेन्द्रहारम् सदावसन्तं हृदयारविन्दे भवं भवानीसहितं नमामि ॥"),
    ],
    "Garuda Purana": [
        (1, 1, "ॐ नमो भगवते वासुदेवाय ।"),
        (1, 2, "धर्मार्थकाममोक्षाणामारोग्यं मूलमुत्तमम् ।"),
        (1, 86, "अयं निजः परो वेति गणना लघुचेतसाम् उदारचरितानां तु वसुधैव कुटुम्बकम् ॥"),
    ],
    "Markandeya Purana": [
        (1, 1, "जयन्ती मङ्गला काली भद्रकाली कपालिनी दुर्गा क्षमा शिवा धात्री स्वाहा स्वधा नमोऽस्तु ते ॥"),
        (81, 1, "सर्वमङ्गलमाङ्गल्ये शिवे सर्वार्थसाधिके शरण्ये त्र्यम्बके गौरि नारायणि नमोऽस्तु ते ॥"),
        (81, 2, "या देवी सर्वभूतेषु शक्तिरूपेण संस्थिता नमस्तस्यै नमस्तस्यै नमस्तस्यै नमो नमः ॥"),
    ],
}


def _existing_keys(db, category: str, work: str) -> Set[Tuple[str, int, int]]:
    rows = (
        db.query(Text.sub_work, Text.chapter, Text.verse)
        .filter(Text.category == category, Text.work == work)
        .all()
    )
    return {(r[0], r[1], r[2]) for r in rows}


def seed_upanishads(db, dry_run: bool = False) -> int:
    existing = _existing_keys(db, "upanishad", "Upanishads")
    created = 0

    for upanishad_name, mantras in UPANISHAD_MANTRAS.items():
        for chapter, verse, sanskrit in mantras:
            key = (upanishad_name, chapter, verse)
            if key in existing:
                continue
            record = Text(
                category="upanishad",
                work="Upanishads",
                sub_work=upanishad_name,
                chapter=chapter,
                verse=verse,
                sanskrit=sanskrit,
                source="Traditional",
                content=sanskrit,
            )
            db.add(record)
            existing.add(key)
            created += 1

    if dry_run:
        db.rollback()
    else:
        db.commit()
    return created


def seed_puranas(db, dry_run: bool = False) -> int:
    existing_bp = _existing_keys(db, "purana", "Bhagavata Purana")
    existing_vp = _existing_keys(db, "purana", "Vishnu Purana")
    existing_sp = _existing_keys(db, "purana", "Shiva Purana")
    existing_gp = _existing_keys(db, "purana", "Garuda Purana")
    existing_mp = _existing_keys(db, "purana", "Markandeya Purana")

    existing_map = {
        "Bhagavata Purana": existing_bp,
        "Vishnu Purana": existing_vp,
        "Shiva Purana": existing_sp,
        "Garuda Purana": existing_gp,
        "Markandeya Purana": existing_mp,
    }

    created = 0
    for purana_name, verses in PURANA_VERSES.items():
        existing = existing_map.get(purana_name, set())
        sub_work = purana_name

        for chapter, verse, sanskrit in verses:
            key = (sub_work, chapter, verse)
            if key in existing:
                continue
            record = Text(
                category="purana",
                work=purana_name,
                sub_work=sub_work,
                chapter=chapter,
                verse=verse,
                sanskrit=sanskrit,
                source="Traditional",
                content=sanskrit,
            )
            db.add(record)
            existing.add(key)
            created += 1

    if dry_run:
        db.rollback()
    else:
        db.commit()
    return created


def seed_ramcharitmanas(db, dry_run: bool = False) -> int:
    """Seed Ramcharitmanas from the Hindi JSON files."""
    data_path = os.path.join(DATA_ROOT, "Ramcharitmanas")
    if not os.path.exists(data_path):
        print("  ⚠ Ramcharitmanas data not found")
        return 0

    existing = _existing_keys(db, "itihasa", "Ramcharitmanas")
    created = 0

    kanda_names = {
        1: "Bal Kand",
        2: "Ayodhya Kand",
        3: "Aranya Kand",
        4: "Kishkindha Kand",
        5: "Sundar Kand",
        6: "Lanka Kand",
        7: "Uttar Kand",
    }

    for fname in sorted(os.listdir(data_path)):
        if not fname.endswith(".json"):
            continue

        # Extract kanda number from filename
        digits = re.findall(r'\d+', fname)
        kanda_num = int(digits[0]) if digits else 0
        if kanda_num == 0:
            continue

        sub_work = kanda_names.get(kanda_num, f"Kanda {kanda_num}")
        filepath = os.path.join(data_path, fname)

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  ⚠ Skipping {fname}: {e}")
            continue

        if not isinstance(data, list):
            data = [data]

        verse_counter = 0
        for item in data:
            if not isinstance(item, dict):
                continue
            content = item.get("content", "")
            if not content or not content.strip():
                continue

            verse_counter += 1
            key = (sub_work, kanda_num, verse_counter)
            if key in existing:
                continue

            record = Text(
                category="itihasa",
                work="Ramcharitmanas",
                sub_work=sub_work,
                chapter=kanda_num,
                verse=verse_counter,
                sanskrit=content.strip(),
                source="DharmicData",
                content=content.strip(),
            )
            db.add(record)
            existing.add(key)
            created += 1

    if dry_run:
        db.rollback()
    else:
        db.commit()
    return created


def main():
    parser = argparse.ArgumentParser(description="Seed Upanishads, Puranas, and Ramcharitmanas")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        print("\n📖 Seeding Upanishads (10 principal texts)...")
        count = seed_upanishads(db, args.dry_run)
        print(f"   ✅ {count} mantras {'(dry run)' if args.dry_run else 'created'}")

        print("\n📖 Seeding Puranas (5 major texts)...")
        count = seed_puranas(db, args.dry_run)
        print(f"   ✅ {count} verses {'(dry run)' if args.dry_run else 'created'}")

        print("\n📖 Seeding Ramcharitmanas (7 kandas)...")
        count = seed_ramcharitmanas(db, args.dry_run)
        print(f"   ✅ {count} verses {'(dry run)' if args.dry_run else 'created'}")

        status = "🔍 DRY RUN" if args.dry_run else "✨ DONE"
        print(f"\n{status}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
