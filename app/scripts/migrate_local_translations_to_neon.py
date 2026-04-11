import psycopg2
from psycopg2.extras import execute_values
import uuid

LOCAL_DB_URL = "postgresql://app_user:strongpassword@localhost:5432/shlokas_db"
NEON_DB_URL = "postgresql://neondb_owner:npg_AX2egj0KUPBo@ep-summer-salad-am7jhsnn-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require"

def migrate_translations_fast():
    print("Connecting...")
    conn_local = psycopg2.connect(LOCAL_DB_URL)
    conn_neon = psycopg2.connect(NEON_DB_URL)
    
    cur_local = conn_local.cursor()
    cur_neon = conn_neon.cursor()
    
    # 1. Fetch Neon Text IDs lookup map
    print("Fetching Neon DB texts mapping...")
    cur_neon.execute("SELECT id, work, sub_work, chapter, verse FROM texts;")
    neon_text_map = {}
    for row in cur_neon.fetchall():
        neon_text_map[(row[1], row[2], row[3], row[4])] = row[0]
    print(f"Loaded {len(neon_text_map)} Neon DB texts.")

    # 2. Fetch existing translations in Neon to avoid duplicates
    print("Fetching existing Neon DB translations...")
    cur_neon.execute("SELECT text_id, language FROM translations;")
    existing_neon = set((row[0], row[1]) for row in cur_neon.fetchall())
    print(f"Loaded {len(existing_neon)} existing Neon DB translations.")

    # 3. Retrieve local translations joined with text metadata
    print("Fetching Local DB translations...")
    query = """
        SELECT t.work, t.sub_work, t.chapter, t.verse, 
               tr.language, tr.translation, tr.commentary, tr.generated_by
        FROM translations tr
        JOIN texts t ON tr.text_id = t.id;
    """
    cur_local.execute(query)
    local_data = cur_local.fetchall()
    print(f"Found {len(local_data)} translations in Local DB.")

    to_insert = []
    skipped_dup = 0
    skipped_missing_text = 0

    for row in local_data:
        work, sub_work, chapter, verse, language, translation, commentary, generated_by = row
        key = (work, sub_work, chapter, verse)
        
        neon_text_id = neon_text_map.get(key)
        if not neon_text_id:
            skipped_missing_text += 1
            continue
            
        if (neon_text_id, language) in existing_neon:
            skipped_dup += 1
            continue
            
        to_insert.append((
            str(uuid.uuid4()), neon_text_id, language, translation, commentary, generated_by
        ))
        existing_neon.add((neon_text_id, language))
        
    print(f"Prepared {len(to_insert)} translations for bulk insert.")
    print(f"Skipped {skipped_dup} duplicates.")
    print(f"Skipped {skipped_missing_text} missing texts.")

    if to_insert:
        print("Inserting...")
        insert_query = """
            INSERT INTO translations (id, text_id, language, translation, commentary, generated_by)
            VALUES %s
        """
        for i in range(0, len(to_insert), 1000):
            batch = to_insert[i:i + 1000]
            execute_values(cur_neon, insert_query, batch)
            conn_neon.commit()
            print(f"Migrated batch {i} to {i + len(batch)}...")
        print("Bulk insert complete.")

    cur_local.close()
    cur_neon.close()
    conn_local.close()
    conn_neon.close()
    print("Done!")

if __name__ == "__main__":
    migrate_translations_fast()
