import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

# R2 Configuration
ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
ENDPOINT_URL = os.getenv("R2_S3_ENDPOINT")
BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "shlokas-audio")

def migrate_audio():
    if not all([ACCESS_KEY_ID, SECRET_ACCESS_KEY, ENDPOINT_URL]):
        print("Error: Missing R2 credentials in .env")
        return

    # Initialize S3 client for R2
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto"  # R2 uses 'auto'
    )

    audio_dir = "audio"
    if not os.path.exists(audio_dir):
        print(f"Error: Directory '{audio_dir}' not found.")
        return

    print(f"Starting migration of '{audio_dir}' to bucket '{BUCKET_NAME}'...")

    count = 0
    for root, dirs, files in os.walk(audio_dir):
        for file in files:
            local_path = os.path.join(root, file)
            # Create a key that matches the local structure
            # e.g., audio/uuid/English.aiff
            key = local_path.replace("\\", "/") # Ensure forward slashes for S3
            
            print(f"Uploading: {local_path} -> {key}")
            try:
                s3.upload_file(local_path, BUCKET_NAME, key)
                count += 1
            except Exception as e:
                print(f"Failed to upload {local_path}: {e}")

    print(f"Migration complete. Uploaded {count} files.")

if __name__ == "__main__":
    migrate_audio()
