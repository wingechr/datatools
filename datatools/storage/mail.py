"""Main script"""

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from email import encoders, message_from_bytes
from email.message import Message
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO
import json
import logging
import re
import smtplib
import ssl
from typing import TYPE_CHECKING
from urllib.parse import unquote
from zipfile import ZipFile

import dateutil.parser
from imapclient import SEEN, IMAPClient

if TYPE_CHECKING:
    from datatools.storage.base import DataStorage

DEFAULT_IMAP_PORT: int = 143
DEFAULT_SMTP_PORT: int = 25
DEFAULT_IMAP_FOLDER: str = "INBOX"


def split_email(mail: str) -> tuple[str, str, str]:
    """Example

    >>> split_email("user:password@host.com")
    ('user', 'password', 'host.com')
    """
    user_passwd, host = mail.split("@")
    user, passwd = user_passwd.split(":")
    passwd = unquote(passwd)
    return user, passwd, host


@dataclass(slots=True)
class MailMetadata:
    """TODO"""

    MessageID: str | None
    FromForwarded: str | None
    Subject: str | None
    Date: str | None
    FromOriginal: str | None = None
    MessageText: str | None = None

    @property
    def unique_name(self) -> str:
        """TODO"""
        date_val = self.Date or "MISSING-DATE"
        from_val = self.FromOriginal or self.FromForwarded or "MISSING-FROM"
        id_val = self.MessageID or "MISSING-ID"
        return f"{date_val}_{from_val}_{id_val}"

    def create_for_attachment(self, attachment: "Attachment") -> dict:
        """TODO"""
        return asdict(self) | {
            "fileName": attachment.filename,
            "mediatype": attachment.contentType,
            "bytes": len(attachment.data or b""),
        }


@dataclass(slots=True)
class Attachment:
    """TODO"""

    data: str | bytes | None = None
    filename: str | None = None
    contentType: str | None = None

    def has_name_and_data(self) -> bool:
        """TODO"""
        if not self.filename:
            logging.warning("no filename")
            return False
        if not self.data:
            logging.warning(f"no data in {self.filename}")
            return False
        return True


class MailAttachmentHandler(ABC):
    """TODO"""

    def __init__(
        self,
        login_mail: str,
        email_whitelist: list[str],
        imap_port: int = DEFAULT_IMAP_PORT,
        imap_folder: str = DEFAULT_IMAP_FOLDER,
        idle_check_timeout: int = 10,
    ):  #
        user, passwd, host = split_email(login_mail)

        self.login_mail = f"{user}@{host}"
        self.password = passwd
        self.host = host
        self.imap_port = imap_port
        self.idle_check_timeout = idle_check_timeout
        self.imap_folder = imap_folder
        self.email_whitelist_lower = {
            x.lower() for x in set(email_whitelist) | {self.login_mail}
        }

    def try_get_date_str(self, message: Message) -> str | None:
        """TODO"""
        date = message.get("Date")
        if not date:
            return None
        try:
            return dateutil.parser.parse(date).strftime("%Y-%m-%d")
        except Exception:
            return str(date)

    def extract_mail(self, mail: str | None) -> str | None:
        """TODO"""
        if not mail:
            return ""
        match = re.match(".*<([^>@]+@[^>@]+)>", mail)
        if not match:
            logging.warning(f"Unexpected mail string: {mail}")
            return mail

        return match.groups()[0]

    def try_extract_from_mail(self, mail: str | None) -> str | None:
        """TODO"""
        if not mail:
            return ""
        mail = re.sub(r"\s+", " ", mail)
        match = re.match(".*From:[^<]*<([^>@]+@[^>@]+)>", mail)
        if not match:
            logging.warning("Could not get From mail")
            return None

        return match.groups()[0]

    def connect_client(self) -> IMAPClient:
        """TODO"""
        logging.debug("connect_client: start")
        # Create TLS context
        ssl_context = ssl.create_default_context()
        logging.info(
            "connect_client: login %s:%s => %s",
            self.login_mail,
            self.imap_port,
            self.imap_folder,
        )
        client = IMAPClient(
            host=self.host, port=self.imap_port, ssl=False, use_uid=True
        )
        client.starttls(ssl_context)  # upgrade to TLS
        client.login(self.login_mail, self.password)
        client.select_folder(self.imap_folder)
        logging.debug("connect_client: ok")
        return client

    def check(self, client: IMAPClient, remove_flag_seen: bool = False):
        """TODO"""
        logging.debug("check: checking for new messages...")
        messages: list[int] = client.search("UNSEEN")
        for uid, message_data in client.fetch(messages, "RFC822").items():
            if b"RFC822" not in message_data:
                logging.info("check: skipping %s", uid)
                continue
            message_bytes: bytes = message_data[b"RFC822"]  # type:ignore
            message = message_from_bytes(message_bytes)

            self.handle_message(message)

            if remove_flag_seen:
                client.remove_flags(uid, [SEEN])
        logging.debug("check: done")

    def get_metadata_file(self, metadata_dict: dict) -> tuple[str, bytes]:
        """TODO"""
        filename = metadata_dict["name"]
        bytes_metadata = json.dumps(
            metadata_dict, indent=2, ensure_ascii=False
        ).encode()
        filename_metadata = filename + ".metadata.json"

        return filename_metadata, bytes_metadata

    def handle_message_part(self, part: Message) -> Attachment:
        """TODO"""
        content_type = part.get_content_type()
        disposition = part.get_content_disposition()
        charset = part.get_content_charset()
        payload_bytes: bytes = part.get_payload(decode=True)  # type:ignore
        filename = part.get_filename()
        if (
            payload_bytes
            and charset
            and content_type.startswith("text/")
            and disposition != "attachment"
        ):
            payload_text = payload_bytes.decode(charset, errors="replace")
            return Attachment(data=payload_text)
        elif payload_bytes and disposition == "attachment" and filename:
            return Attachment(
                data=payload_bytes, filename=filename, contentType=content_type
            )
        else:
            return Attachment()

    def handle_message(self, message: Message):
        """TODO"""
        from_mail = self.extract_mail(message.get("From")) or ""
        if from_mail.lower() not in self.email_whitelist_lower:
            logging.info("Ignore mail from : %s", from_mail)
            return

        metadata = MailMetadata(
            MessageID=self.extract_mail(message.get("Message-ID")),
            FromForwarded=from_mail,
            Subject=message.get("Subject"),
            Date=self.try_get_date_str(message),
        )
        logging.info("handle_message: %s", metadata)

        # Iterate through parts and return the first text/plain part
        attachments = []
        texts = []
        for part in message.walk():
            attachment = self.handle_message_part(part)
            if attachment.filename:
                attachments.append(attachment)
            elif attachment.data:
                texts.append(attachment.data)

        text = "\n\n".join(texts)
        from_original = self.try_extract_from_mail(text)

        if from_original:
            metadata.FromOriginal = from_original
        metadata.MessageText = text

        if attachments:
            self.handle_attachments(attachments, metadata)

    @abstractmethod
    def handle_attachments(
        self, attachments: list[Attachment], metadata: MailMetadata
    ): ...

    def serve(self):
        """TODO"""
        client = self.connect_client()
        while True:
            self.check(client)
            client.idle_check(timeout=self.idle_check_timeout)


class MailAttachmentForwadHandler(MailAttachmentHandler):
    """TODO"""

    def __init__(
        self,
        login_mail: str,
        email_whitelist: list[str],
        imap_port: int = DEFAULT_IMAP_PORT,
        imap_folder: str = DEFAULT_IMAP_FOLDER,
        smtp_port: int = DEFAULT_SMTP_PORT,
        idle_check_timeout: int = 10,
    ):
        super().__init__(
            login_mail=login_mail,
            email_whitelist=email_whitelist,
            imap_port=imap_port,
            idle_check_timeout=idle_check_timeout,
            imap_folder=imap_folder,
        )
        self.smtp_port = smtp_port

    def _create_return_attachment(
        self, attachments: list[Attachment], metadata: MailMetadata
    ) -> bytes:
        """TODO"""
        buf = BytesIO()
        with ZipFile(buf, mode="w") as zf:
            for attachment in attachments:
                if not attachment.has_name_and_data():
                    continue
                zf.writestr(attachment.filename, attachment.data)  # type:ignore - checked above
                zf.writestr(
                    *self.get_metadata_file(metadata.create_for_attachment(attachment))
                )

        buf.seek(0)
        zip_data = buf.read()
        return zip_data

    def handle_attachments(self, attachments: list[Attachment], metadata: MailMetadata):
        """TODO"""
        # send back
        to_address = metadata.FromForwarded
        if not to_address:
            logging.error("Failed to parse From (return) address")
            return

        attachment_zip_data = self._create_return_attachment(attachments, metadata)

        msg = MIMEMultipart()
        msg["From"] = self.login_mail
        msg["To"] = to_address
        msg["Subject"] = f"Re: {metadata.Subject}"

        # Add body
        body = ""
        msg.attach(MIMEText(body, "plain"))

        # Attach file
        part = MIMEBase("application", "zip")

        part.set_payload(attachment_zip_data)
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition", f'attachment; filename="{metadata.unique_name}.zip"'
        )
        msg.attach(part)

        # Send email
        server = smtplib.SMTP(self.host, self.smtp_port)
        server.starttls()
        server.login(self.login_mail, self.password)
        server.send_message(msg)
        server.quit()


class MailAttachmentStorageHandler(MailAttachmentHandler):
    """TODO"""

    def __init__(
        self,
        storage: "DataStorage",
        login_mail: str,
        email_whitelist: list[str],
        imap_port: int = DEFAULT_IMAP_PORT,
        imap_folder: str = DEFAULT_IMAP_FOLDER,
        idle_check_timeout: int = 10,
    ):
        super().__init__(
            login_mail=login_mail,
            email_whitelist=email_whitelist,
            imap_port=imap_port,
            idle_check_timeout=idle_check_timeout,
            imap_folder=imap_folder,
        )
        self.storage = storage

    def handle_attachments(self, attachments: list[Attachment], metadata: MailMetadata):
        """TODO"""
        for attachment in attachments:
            if not attachment.has_name_and_data():
                continue

            def make_get_mail(attachment: Attachment):
                def get_mail() -> bytes:
                    return attachment.data  # type:ignore checked aboveata

                return get_mail

            def make_get_metadata(attachment: Attachment):
                metadata_dict = metadata.create_for_attachment(attachment)

                def get_metadata(_) -> dict:
                    return metadata_dict

                return get_metadata

            task = self.storage.task(
                make_get_mail(attachment),
                metadata_generator=make_get_metadata(attachment),
            )

            resource_name = f"{metadata.unique_name}/{attachment.filename}"
            task(resource_name)
