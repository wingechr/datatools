"""Main script"""

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from email import message_from_bytes, policy
from email.message import Message
from email.utils import parseaddr
import logging
import ssl
from typing import TYPE_CHECKING
from urllib.parse import unquote

import dateutil
from imapclient import IMAPClient

from datatools import AnnotatedFunction
from datatools.utils import get_plain_text_msg_and_original_from

if TYPE_CHECKING:
    from datatools.storage.base import DataStorage

DEFAULT_IMAP_PORT: int = 143
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


class MailAttachmentHandler(ABC):
    """TODO"""

    def __init__(
        self,
        login_mail: str,
        email_whitelist: list[str],
        imap_port: int = DEFAULT_IMAP_PORT,
        imap_folder: str = DEFAULT_IMAP_FOLDER,
        idle_check_timeout: int = 10,
        use_ssl: bool = False,
        use_starttls: bool = True,
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
        self.use_ssl = use_ssl
        self.use_starttls = use_starttls

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
            host=self.host, port=self.imap_port, ssl=self.use_ssl, use_uid=True
        )
        if self.use_starttls:
            client.starttls(ssl_context)  # upgrade to TLS # pragma: no cover
        client.login(self.login_mail, self.password)
        client.select_folder(self.imap_folder)
        logging.debug("connect_client: ok")
        return client

    def _check(self, client: IMAPClient):
        """TODO"""
        logging.debug("check: checking for new messages...")
        messages: list[int] = client.search("UNSEEN")
        for uid, message_data in client.fetch(messages, "RFC822").items():
            logging.info("found message %s", uid)
            message_bytes: bytes = message_data[b"RFC822"]  # type:ignore -> Exception
            message = message_from_bytes(message_bytes, policy=policy.default)
            self.handle_message(message)
        logging.debug("check: done")

    def handle_message_part(self, part: Message) -> Attachment:
        """TODO"""
        content_type = part.get_content_type()
        disposition = part.get_content_disposition()
        charset = part.get_content_charset()
        payload_bytes: bytes = part.get_payload(decode=True)  # type:ignore -> Exception
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
        _name, from_mail = parseaddr(message["From"])
        if from_mail.lower() not in self.email_whitelist_lower:
            logging.info("Ignore mail from : %s", from_mail)
            return
        _, message_id = parseaddr(message["Message-ID"])  # ususally <x@y>
        metadata = MailMetadata(
            MessageID=message_id,
            FromForwarded=from_mail,
            Subject=message.get("Subject"),
            Date=dateutil.parser.parse(message["Date"]).strftime("%Y-%m-%d"),
        )
        logging.info("handle_message: %s", metadata)

        # Iterate through parts and return the first text/plain part
        attachments = []
        texts = []
        for part in message.walk():
            text_or_attachment = self.handle_message_part(part)
            if text_or_attachment.filename:
                logging.info(
                    "found attachment: %s (%s)",
                    text_or_attachment.filename,
                    text_or_attachment.contentType,
                )
                attachments.append(text_or_attachment)
            elif text_or_attachment.data:
                logging.info("found text (%s)", text_or_attachment.contentType)
                texts.append(text_or_attachment.data)
            else:
                logging.info("Skipping empty part")

        text: str = "\n\n".join(texts)

        text, from_original = get_plain_text_msg_and_original_from(text)

        metadata.FromOriginal = from_original
        metadata.MessageText = text

        if from_original:
            logging.info("found from_original: %s", from_original)

        logging.info(text)

        if attachments:
            self.handle_attachments(attachments, metadata)

    @abstractmethod
    def handle_attachments(
        self, attachments: list[Attachment], metadata: MailMetadata
    ): ...

    def serve_forever(self):  # pragma: no cover
        """TODO"""
        client = self.connect_client()
        while True:
            self._check(client)
            client.idle_check(timeout=self.idle_check_timeout)

    def check(self):
        """TODO"""
        client = self.connect_client()
        self._check(client)


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
        use_ssl: bool = False,
        use_starttls: bool = True,
    ):
        super().__init__(
            login_mail=login_mail,
            email_whitelist=email_whitelist,
            imap_port=imap_port,
            idle_check_timeout=idle_check_timeout,
            imap_folder=imap_folder,
            use_ssl=use_ssl,
            use_starttls=use_starttls,
        )
        self.storage = storage

    def handle_attachments(self, attachments: list[Attachment], metadata: MailMetadata):
        """TODO"""
        for attachment in attachments:

            def make_get_mail(attachment: Attachment):
                @AnnotatedFunction.wrap(function_id="MAIL")
                def get_mail() -> bytes:
                    return attachment.data  # type:ignore -> Exception

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
            resource_name = resource_name.lower()
            task(resource_name)
