from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from logger_config import setup_logger

creds = service_account.Credentials.from_service_account_file(
    "credentials.json", scopes=["https://www.googleapis.com/auth/drive"]
)

drive_service = build("drive", "v3", credentials=creds)


def upload_files(
    path: str, folder_id: str = "1krWR2x7w2hmxbchPaZyBMknHmaduMiD4"
) -> str:
    file_metadata = {
        "name": path.split("/")[-1],  # Usa el nombre del archivo
    }
    if folder_id:
        file_metadata["parents"] = [folder_id]  # Carpeta destino (opcional)
    media = MediaFileUpload(
        path,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    file = (
        drive_service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )

    print(f"✅ Subido: {path} → ID: {file.get('id')}")

    return file.get("id")
