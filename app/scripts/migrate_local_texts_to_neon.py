import psycopg2
from psycopg2.extras import execute_values
import uuid

LOCAL_DB_URL = "postgresql://app_user:strongpassword@localhost:5432/shlokas_db"
NEON_DB_URL = "postgresql://neondb_owner:npg_AX2egj0KUPBo@ep-summer-salad-am7jhsnn-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require"

def migrate_texts_fast():
    print("Connecting...")
    conn_local = psycopg2.connect(LOCAL_DB_URL)
    conn_neon = psycopg2.connect(NEON_DB_URL)
    
    cur_local = conn_local.cursor()
    cur_neon = conn_neon.cursor()
    
    # Fetch existing neon texts
    cur_neon.execute("SELECT work, sub_work, chapter, verse FROM texts;")
    existing_keys = set((row[0], row[1], row[2], row[3]) for row in cur_neon.fetchall())
    print(f"Loaded {len(existing_keys)} existing texts from Neon.")
    
    # Fetch all local texts
    cur_local.execute("SELECT category, work, sub_work, chapter, verse, sanskrit, source, content FROM texts;")
    local_texts = cur_local.fetchall()
    print(f"Found {len(local_texts)} texts in local DB.")
    
    to_insert = []
    skipped = 0
    
    for row in local_texts:
        category, work, sub_work, chapter, verse, sanskrit, source, content = row
        key = (work, sub_work, chapter, verse)
        if key in existing_keys:
            skipped += 1
            continue
        to_insert.append((
            str(uuid.uuid4()), category, work, sub_work, chapter, verse, sanskrit, source, content
        ))
        existing_keys.add(key)
        
    print(f"Prepared {len(to_insert)} newly missing texts for bulk insert.")
    
    if to_insert:
        query = """INSERT INTO texts (id, category, work, sub_work, chapter, verse, sanskrit, source, content) VALUES %s"""
        for i in range(0, len(to_insert), 1000):
            batch = to_insert[i:i + 1000]
            execute_values(cur_neon, query, batch)
            conn_neon.commit()
            print(f"Migrated batch {i} to {i + len(batch)}...")
    
    print("Migration of Texts complete!")
    
    cur_local.close()
    cur_neon.close()
    conn_local.close()
    conn_neon.close()

if __name__ == "__main__":
    migrate_texts_fast()
