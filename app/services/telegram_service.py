import os
import requests


def send_telegram_message(message: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    response = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
    )

    print("Message status:", response.status_code)
    print("Message response:", response.text)


def send_telegram_document(file_buffer, filename: str, caption: str = None):
    """
    Sends a PDF or file to Telegram.
    """

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{token}/sendDocument"

    files = {
        "document": (filename, file_buffer, "application/pdf")
    }

    data = {
        "chat_id": chat_id,
        "caption": caption or ""
    }

    response = requests.post(url, data=data, files=files)

    print("Document status:", response.status_code)
    print("Document response:", response.text)