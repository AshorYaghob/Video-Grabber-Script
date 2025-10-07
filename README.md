# Google Drive Video Thumbnail Grabber

This Python script automates the process of scanning Google Drive folders for video files, capturing a frame at a specified timestamp using OpenCV, uploading the resulting thumbnail back to Drive, and logging details (thumbnail name, path, and link) into a Google Sheet.

All API credentials and folder IDs are managed via environment variables — no secrets are hardcoded.

## Usage

1. Set up a Google Cloud service account with Drive and Sheets API access.  
2. Export your credentials and configuration as environment variables (see `.env.example`).  
3. Run:
   ```bash
   python grabber.py

## Dependencies

- google-api-python-client  
- google-auth  
- opencv-python
