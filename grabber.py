import os
import io
import time
import json
import cv2  # OpenCV for video processing
from typing import Optional, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from socket import timeout as SocketTimeout  # for robust timeout catching

# -----------------------------
# Configuration via environment
# -----------------------------
# Required (provide in environment or .env):
ENV_SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")  # e.g., /path/to/Service_Account.json
ENV_SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")  # optional: raw JSON string (alternative to file)
START_FOLDER_ID = os.getenv("START_FOLDER_ID", "FOLDER_ID_PLACEHOLDER")
THUMBNAIL_FOLDER_ID = os.getenv("THUMBNAIL_FOLDER_ID", "THUMBNAIL_FOLDER_ID_PLACEHOLDER")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "SPREADSHEET_ID_PLACEHOLDER")
SHEET_NAME = os.getenv("SHEET_NAME", "Sheet1")

# The frame to capture from the video (in seconds). Can be float.
CAPTURE_TIMESTAMP_SECONDS = float(os.getenv("CAPTURE_TIMESTAMP_SECONDS", "2"))

# Scopes define the level of access requested (Drive + Sheets).
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

def _load_credentials():
    """
    Load service account credentials from either:
      1) SERVICE_ACCOUNT_JSON (preferred for containers/CI), or
      2) SERVICE_ACCOUNT_FILE (path on disk).
    """
    if ENV_SERVICE_ACCOUNT_JSON:
        info = json.loads(ENV_SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        return creds

    if not ENV_SERVICE_ACCOUNT_FILE:
        raise RuntimeError(
            "Missing credentials. Set SERVICE_ACCOUNT_JSON or SERVICE_ACCOUNT_FILE."
        )
    creds = service_account.Credentials.from_service_account_file(
        ENV_SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return creds

def authenticate():
    """Authenticate to Google APIs using a Service Account."""
    creds = _load_credentials()
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service

def append_to_sheet(sheets_service, data_row):
    """
    Appends a row of data to the Google Sheet with an automatic retry mechanism.
    """
    if not data_row:
        return

    max_retries = 5
    backoff_seconds = 2  # Initial wait time

    for attempt in range(max_retries):
        try:
            body = {"values": [list(data_row)]}
            sheets_service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()
            print("  ✍️ Successfully wrote to sheet.")
            return  # Exit on success

        except (HttpError, TimeoutError, SocketTimeout) as error:
            print(f"  ⚠️ Attempt {attempt + 1} failed: {error}")
            if attempt + 1 == max_retries:
                print("  ❌ Max retries reached. Could not write to sheet.")
                break

            print(f"  Retrying in {backoff_seconds} seconds...")
            time.sleep(backoff_seconds)
            backoff_seconds *= 2  # Exponential backoff

def process_video(drive_service, video_file, folder_path) -> Optional[Tuple[str, str, str]]:
    """
    Downloads a video, captures a thumbnail frame, uploads it to Drive,
    and returns (thumbnail_name, original_path, webViewLink).
    """
    file_id = video_file.get("id")
    file_name = video_file.get("name")
    print(f"Processing video: {file_name}")

    temp_video_path = None
    thumbnail_path = None

    try:
        # Download the video file into memory
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"  Download {int(status.progress() * 100)}%.")

        fh.seek(0)

        # Create a temporary file path to work with OpenCV
        temp_video_path = f"temp_{file_name}"
        with open(temp_video_path, "wb") as f:
            f.write(fh.read())

        # Use OpenCV to capture a frame
        vidcap = cv2.VideoCapture(temp_video_path)
        fps = vidcap.get(cv2.CAP_PROP_FPS)
        if not fps or fps == 0:
            print(f"  ❌ Could not get FPS for {file_name}. Skipping.")
            vidcap.release()
            return None

        frame_id = int(fps * CAPTURE_TIMESTAMP_SECONDS)
        vidcap.set(cv2.CAP_PROP_POS_FRAMES, frame_id)
        success, image = vidcap.read()
        vidcap.release()

        if not success or image is None:
            print(f"  ❌ Could not grab frame from {file_name}.")
            return None

        # Save the frame as a thumbnail image
        thumbnail_name = f"{os.path.splitext(file_name)[0]}_Thumbnail.jpg"
        thumbnail_path = f"temp_{thumbnail_name}"
        cv2.imwrite(thumbnail_path, image)

        # Upload the thumbnail to Google Drive
        print(f"  ⬆️ Uploading thumbnail: {thumbnail_name}")
        file_metadata = {
            "name": thumbnail_name,
            "parents": [THUMBNAIL_FOLDER_ID],
        }
        media = MediaFileUpload(thumbnail_path, mimetype="image/jpeg", resumable=True)

        uploaded_thumbnail = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        ).execute()

        print(f"  ✅ Thumbnail created with ID: {uploaded_thumbnail.get('id')}")
        return uploaded_thumbnail.get("name"), folder_path, uploaded_thumbnail.get("webViewLink")

    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

    finally:
        # Clean up local temporary files
        try:
            if temp_video_path and os.path.exists(temp_video_path):
                os.remove(temp_video_path)
            if thumbnail_path and os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
        except Exception as cleanup_err:
            print(f"  (cleanup warning) {cleanup_err}")

def traverse_folder(drive_service, sheets_service, folder_id, current_path):
    """Recursively traverses folders to find and process videos."""
    query = f"'{folder_id}' in parents and trashed=false"
    try:
        results = drive_service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora="allDrives",
        ).execute()
        items = results.get("files", [])

        for item in items:
            item_path = f"{current_path}/{item.get('name')}"
            mime = item.get("mimeType", "")
            if mime == "application/vnd.google-apps.folder":
                traverse_folder(drive_service, sheets_service, item.get("id"), item_path)
            elif mime.startswith("video/"):
                log_data = process_video(drive_service, item, current_path)
                print(f"DEBUGGING - Data to be logged: {log_data}")
                append_to_sheet(sheets_service, log_data)

    except HttpError as error:
        print(f"An error occurred: {error}")

def main():
    """Main function to orchestrate the process."""
    if START_FOLDER_ID.endswith("_PLACEHOLDER"):
        raise RuntimeError("Please set START_FOLDER_ID in your environment.")

    if THUMBNAIL_FOLDER_ID.endswith("_PLACEHOLDER"):
        raise RuntimeError("Please set THUMBNAIL_FOLDER_ID in your environment.")

    if SPREADSHEET_ID.endswith("_PLACEHOLDER"):
        raise RuntimeError("Please set SPREADSHEET_ID in your environment.")

    drive_service, sheets_service = authenticate()

    # Optional: ensure header row exists
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME
        ).execute()
        if not result.get("values"):
            append_to_sheet(
                sheets_service, ["Thumbnail Name", "Original Video Path", "Link to Thumbnail"]
            )
    except HttpError as error:
        print(f"Could not check sheet, creating a new one might be needed. Error: {error}")

    # Resolve root folder name and traverse
    root_folder = drive_service.files().get(
        fileId=START_FOLDER_ID, fields="name", supportsAllDrives=True
    ).execute()
    root_name = root_folder.get("name", "<root>")
    print(f"🚀 Starting scan in root folder: {root_name}")
    traverse_folder(drive_service, sheets_service, START_FOLDER_ID, root_name)
    print("✨ Process complete.")

if __name__ == "__main__":
    main()
