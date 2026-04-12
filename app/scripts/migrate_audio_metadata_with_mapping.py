import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

LOCAL_DB_URL = "postgresql://app_user:strongpassword@localhost:5432/shlokas_db"
NEON_DB_URL = os.getenv("DATABASE_URL")

def migrate_audio_with_mapping():
    if not NEON_DB_URL:
        print("Error: DATABASE_URL not set in .env")
        return

    print("Connecting to databases...")
    conn_local = psycopg2.connect(LOCAL_DB_URL)
    cur_local = conn_local.cursor()

    conn_neon = psycopg2.connect(NEON_DB_URL)
    cur_neon = conn_neon.cursor()

    # Step 1: Fetch local audio records along with their verse metadata
    print("Fetching local audio records and metadata...")
    cur_local.execute("""
        SELECT a.id, a.text_id, a.language, a.audio_path, a.voice_type, 
               t.category, t.work, t.sub_work, t.chapter, t.verse
        FROM audio a
        JOIN texts t ON a.text_id = t.id
    """)
    local_records = cur_local.fetchall()
    
    if not local_records:
        print("No audio records found in local DB.")
        return

    print(f"Found {len(local_records)} local records. Building Neon mapping...")

    # Step 2: Fetch all text mappings from Neon to find new text_ids
    print("Fetching Neon text mappings...")
    cur_neon.execute("SELECT id, category, work, sub_work, chapter, verse FROM texts")
    neon_texts = cur_neon.fetchall()
    
    # Map (category, work, sub_work, chapter, verse) -> neon_text_id
    neon_map = {
        (r[1], r[2], r[3], r[4], r[5]): r[0] 
        for r in neon_texts
    }

    # Step 3: Prepare records for insertion with new text_ids
    to_insert = []
    skipped_mapping = 0
    for r in local_records:
        audio_id, old_text_id, lang, path, voice, cat, work, sub, ch, v = r
        
        # Look up new text_id in Neon
        new_text_id = neon_map.get((cat, work, sub, ch, v))
        
        if new_text_id:
            # We keep the same audio_id to maintain consistency if possible
            to_insert.append((audio_id, new_text_id, lang, path, voice))
        else:
            print(f"Warning: Could not find Neon ID for {cat}/{work}/{sub} Ch{ch} V{v}")
            skipped_mapping += 1

    print(f"Prepared {len(to_insert)} records for Neon. (Skipped {skipped_mapping} unmapped).")

    if not to_insert:
        print("Nothing to insert.")
        return

    # Step 4: Bulk insert into Neon
    insert_query = """
    INSERT INTO audio (id, text_id, language, audio_path, voice_type)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        text_id = EXCLUDED.text_id,
        language = EXCLUDED.language,
        audio_path = EXCLUDED.audio_path,
        voice_type = EXCLUDED.voice_type
    """
    
    try:
        execute_values(cur_neon, insert_query, to_insert)
        conn_neon.commit()
        print(f"Successfully migrated {len(to_insert)} audio records to Neon with ID remapping.")
    except Exception as e:
        print(f"Error during migration: {e}")
        conn_neon.rollback()
    finally:
        cur_local.close()
        conn_local.close()
        cur_neon.close()
        conn_neon.close()

if __name__ == "__main__":
    migrate_audio_with_mapping()
