#!/bin/bash
source venv/bin/activate
echo "Starting Data Migration pipeline to Neon..." > migration.log
echo "1. Migrating local Texts to Neon DB..." >> migration.log
python -m app.scripts.migrate_local_texts_to_neon >> migration.log 2>&1
echo "2. Migrating leftover Translations..." >> migration.log
python -m app.scripts.migrate_local_translations_to_neon >> migration.log 2>&1
echo "Migration complete." >> migration.log
