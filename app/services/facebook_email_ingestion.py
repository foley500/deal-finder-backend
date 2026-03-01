import imaplib
import email
import re
from typing import List
from email.header import decode_header


class FacebookEmailIngestion:

    def _init_(
        self,
        host: str,
        username: str,
        password: str,
        mailbox: str = "INBOX"
    ):
        self.host = host
        self.username = username
        self.password = password
        self.mailbox = mailbox

    def fetch_listing_urls(self) -> List[str]:

        urls = []

        mail = imaplib.IMAP4_SSL(self.host)
        mail.login(self.username, self.password)
        mail.select(self.mailbox)

        status, messages = mail.search(None, '(UNSEEN)')

        if status != "OK":
            return []

        for num in messages[0].split():

            _, msg_data = mail.fetch(num, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            # Extract text content
            body = ""

            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type == "text/plain":
                        body += part.get_payload(decode=True).decode(errors="ignore")
            else:
                body += msg.get_payload(decode=True).decode(errors="ignore")

            # Find Marketplace URLs
            found_urls = re.findall(
                r"https://www\.facebook\.com/marketplace/item/[^\s]+",
                body
            )

            urls.extend(found_urls)

        mail.logout()

        return list(set(urls))