"""TODO"""

from __future__ import annotations

from datetime import datetime
from email.mime.message import MIMEMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import email.utils
from email.utils import formataddr
import socketserver

TEST_USER = "test"
TEST_PASSWD = "test"  # noqa: S105
TEST_HOST = "localhost"
TEST_LOGIN = f"{TEST_USER}:{TEST_PASSWD}@{TEST_HOST}"
TEST_MAIL = f"{TEST_USER}@{TEST_HOST}"
TEST_MAIL_ORIGINAL = "original@example.com"
TEST_MAIL_WHILTELISTED = "allowed@example.com"
TEST_DATE = "2001-02-03"


# original message someone is forwarding


def _create_msg(
    mail_forwarded_from: str = TEST_MAIL_WHILTELISTED,
    has_attachment: bool = True,
    was_forwarded: bool = True,
) -> MIMEMultipart:
    date = email.utils.format_datetime(datetime.strptime(TEST_DATE, "%Y-%m-%d"))

    attachment = MIMEText("example file\n", "plain")
    attachment.add_header("Content-Disposition", "attachment", filename="test.txt")

    # original message
    msg = MIMEMultipart()
    msg["From"] = formataddr(("Original", TEST_MAIL_ORIGINAL))
    msg["To"] = mail_forwarded_from
    msg["Subject"] = "Original subject"
    msg["Date"] = "not a valid date"
    msg.attach(MIMEText("Original text.", "plain"))

    # with attachment
    if has_attachment:
        msg.attach(attachment)

    # forward
    msg2 = MIMEMultipart()
    msg2["From"] = mail_forwarded_from
    msg2["To"] = TEST_MAIL
    msg2["Subject"] = "Fwd: Original subject"
    msg2["Date"] = date
    msg2.attach(MIMEText("Forwarded text", "plain"))
    if was_forwarded:
        msg2.attach(MIMEMessage(msg))
    elif has_attachment:
        msg.attach(attachment)

    return msg2


class _Mailbox:
    """In-memory mailbox: msg_id -> raw RFC822 bytes + flags."""

    def __init__(self):
        self.messages: dict[int, bytes] = {}
        self.flags: dict[int, set[str]] = {}
        self._next_id = 1

        # message with invalid forwarder
        self.add_msg(_create_msg(mail_forwarded_from="not_allowed@example.com"))
        # allowed sender, with attachment
        self.add_msg(_create_msg())
        # allowed sender, no attachment
        self.add_msg(_create_msg(has_attachment=False))
        # w/o forwarding
        self.add_msg(_create_msg(was_forwarded=False))

    def add_msg(self, msg: MIMEMultipart):
        msg_id = self._next_id
        msg["Message-ID"] = str(msg_id)
        self._next_id += 1
        self.messages[msg_id] = msg.as_bytes()
        self.flags[msg_id] = set()


class _IMAPHandler(socketserver.StreamRequestHandler):
    def setup(self):
        super().setup()
        self.mailbox = _Mailbox()

    def _send(self, data: bytes):
        self.wfile.write(data)
        self.wfile.flush()

    def handle(self):
        self._send(b"* OK IMAP4rev1 Mock Server Ready\r\n")
        while True:
            line = self.rfile.readline()
            if not line:
                break
            text = line.decode("utf-8", "replace").rstrip("\r\n")
            parts = text.split(maxsplit=2)
            tag, cmd = parts[0], parts[1].upper()
            rest = parts[2] if len(parts) > 2 else ""
            handler = getattr(self, f"cmd_{cmd}")
            handler(tag, rest)

    def cmd_CAPABILITY(self, tag: str, rest: str):
        self._send(b"* CAPABILITY IMAP4rev1\r\n")
        self._send(f"{tag} OK CAPABILITY completed\r\n".encode())

    def cmd_LOGIN(self, tag: str, rest: str):
        # Accepts any credentials
        self._send(f"{tag} OK LOGIN completed\r\n".encode())

    def cmd_SELECT(self, tag: str, rest: str):
        n = len(self.mailbox.messages)
        self._send(f"* {n} EXISTS\r\n".encode())
        self._send(b"* 0 RECENT\r\n")
        self._send(b"* FLAGS (\\Seen \\Answered \\Flagged \\Deleted \\Draft)\r\n")
        self._send(f"{tag} OK [READ-WRITE] SELECT completed\r\n".encode())

    def cmd_UID(self, tag, rest):
        subcmd, _, subrest = rest.partition(" ")
        subcmd = subcmd.upper()

        if subcmd == "SEARCH":
            ids = sorted(self.mailbox.messages)  # UIDs == msg ids in this mock
            self._send(f"* SEARCH {' '.join(map(str, ids))}\r\n".encode())
            self._send(f"{tag} OK UID SEARCH completed\r\n".encode())

        elif subcmd == "FETCH":
            seqset, _, _ = subrest.partition(" ")
            for msg_id in [int(x) for x in seqset.split(",")]:
                raw = self.mailbox.messages[msg_id]
                flags = " ".join(self.mailbox.flags.get(msg_id, ()))
                self._send(
                    f"* {msg_id} FETCH (UID {msg_id} FLAGS ({flags}) RFC822 {{{len(raw)}}}\r\n".encode()  # noqa:E501
                )
                self._send(raw)
                self._send(b")\r\n")
            self._send(f"{tag} OK UID FETCH completed\r\n".encode())

        else:
            raise NotImplementedError()  # pragma: no cover


class MockIMAPServer(socketserver.TCPServer):
    """Minimal fake IMAP server for tests"""

    def __init__(
        self, host: str = "127.0.0.1", port: int = 0, test_mail: str = "test@localhost"
    ):
        self.test_mail = test_mail
        super().__init__((host, port), _IMAPHandler)
