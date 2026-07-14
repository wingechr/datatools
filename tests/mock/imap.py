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


def _create_msg() -> MIMEMultipart:
    date = email.utils.format_datetime(datetime.strptime(TEST_DATE, "%Y-%m-%d"))

    # original message
    msg = MIMEMultipart()
    # msg["Message-ID"] = email.utils.make_msgid()
    msg["From"] = formataddr(("Original", TEST_MAIL_ORIGINAL))
    msg["To"] = TEST_MAIL_WHILTELISTED
    msg["Subject"] = "Original subject"
    msg["Date"] = date
    msg.attach(MIMEText("Original text.", "plain"))

    # with attachment
    attachment = MIMEText("example file\n", "plain")
    attachment.add_header("Content-Disposition", "attachment", filename="test.txt")
    msg.attach(attachment)

    # forward
    fwd = MIMEMultipart()
    # fwd["Message-ID"] = str(msg_id)
    fwd["From"] = TEST_MAIL_WHILTELISTED
    fwd["To"] = TEST_MAIL
    fwd["Subject"] = "Fwd: Original subject"
    fwd["Date"] = date

    fwd.attach(MIMEText("Forwarded text", "plain"))
    fwd.attach(MIMEMessage(msg))

    return fwd


class _Mailbox:
    """In-memory mailbox: msg_id -> raw RFC822 bytes + flags."""

    def __init__(self):
        self.messages: dict[int, bytes] = {}
        self.flags: dict[int, set[str]] = {}
        self._next_id = 1

        self.add_msg(_create_msg())

    def add_msg(self, msg: MIMEMultipart):
        msg_id = self._next_id
        msg["Message-ID"] = str(msg_id)
        self._next_id += 1
        self.messages[msg_id] = msg.as_bytes()
        self.flags[msg_id] = set()


class _StopConnection(Exception):
    pass


class _IMAPHandler(socketserver.StreamRequestHandler):
    def setup(self):
        super().setup()
        self.mailbox = _Mailbox()

    def handle(self):
        self._send(b"* OK IMAP4rev1 Mock Server Ready\r\n")
        while True:
            line = self.rfile.readline()
            if not line:
                break
            text = line.decode("utf-8", "replace").rstrip("\r\n")
            if not text:
                continue
            parts = text.split(maxsplit=2)
            if len(parts) < 2:
                continue
            tag, cmd = parts[0], parts[1].upper()
            rest = parts[2] if len(parts) > 2 else ""

            handler = getattr(self, f"cmd_{cmd}", None)
            if handler is None:
                self._send(f"{tag} BAD Unknown command\r\n".encode())
                continue
            try:
                handler(tag, rest)
            except _StopConnection:
                break

    # -- command handlers ---------------------------------------------

    def cmd_CAPABILITY(self, tag: str, rest: str):
        self._send(b"* CAPABILITY IMAP4rev1\r\n")
        self._send(f"{tag} OK CAPABILITY completed\r\n".encode())

    def cmd_LOGIN(self, tag: str, rest: str):
        # Accepts any credentials -- add your own check here if needed.
        if not rest.startswith(TEST_MAIL):
            raise Exception("Not authorized")
        self._send(f"{tag} OK LOGIN completed\r\n".encode())

    def __cmd_NOOP(self, tag: str, rest: str):
        self._send(f"{tag} OK NOOP completed\r\n".encode())

    def __cmd_LIST(self, tag: str, rest: str):
        self._send(b'* LIST (\\HasNoChildren) "/" "INBOX"\r\n')
        self._send(f"{tag} OK LIST completed\r\n".encode())

    def cmd_SELECT(self, tag: str, rest: str):
        n = len(self.mailbox.messages)
        self._send(f"* {n} EXISTS\r\n".encode())
        self._send(b"* 0 RECENT\r\n")
        self._send(b"* FLAGS (\\Seen \\Answered \\Flagged \\Deleted \\Draft)\r\n")
        self._send(f"{tag} OK [READ-WRITE] SELECT completed\r\n".encode())

    cmd_EXAMINE = cmd_SELECT

    def cmd_UID(self, tag, rest):
        subcmd, _, subrest = rest.partition(" ")
        subcmd = subcmd.upper()

        if subcmd == "SEARCH":
            ids = sorted(self.mailbox.messages)  # UIDs == msg ids in this mock
            self._send(f"* SEARCH {' '.join(map(str, ids))}\r\n".encode())
            self._send(f"{tag} OK UID SEARCH completed\r\n".encode())

        elif subcmd == "FETCH":
            seqset, _, _ = subrest.partition(" ")
            for i in self._expand_seqset(seqset):
                raw = self.mailbox.messages.get(i)
                if raw is None:
                    continue
                flags = " ".join(self.mailbox.flags.get(i, ()))
                self._send(
                    f"* {i} FETCH (UID {i} FLAGS ({flags}) RFC822 {{{len(raw)}}}\r\n".encode()  # noqa:E501
                )
                self._send(raw)
                self._send(b")\r\n")
            self._send(f"{tag} OK UID FETCH completed\r\n".encode())

        else:
            self._send(f"{tag} BAD Unsupported UID subcommand\r\n".encode())

    def cmd_SEARCH(self, tag: str, rest: str):
        ids = " ".join(str(i) for i in sorted(self.mailbox.messages))
        self._send(f"* SEARCH {ids}\r\n".encode())
        self._send(f"{tag} OK SEARCH completed\r\n".encode())

    def cmd_FETCH(self, tag: str, rest: str):
        # rest looks like: "1 (RFC822)" or "1:2 (FLAGS RFC822.SIZE)"
        seqset, _, _ = rest.partition(" ")
        for i in self._expand_seqset(seqset):
            raw = self.mailbox.messages.get(i)
            if raw is None:
                continue
            flags = " ".join(self.mailbox.flags.get(i, ()))
            self._send(
                f"* {i} FETCH (FLAGS ({flags}) RFC822 {{{len(raw)}}}\r\n".encode()
            )
            self._send(raw)
            self._send(b")\r\n")
        self._send(f"{tag} OK FETCH completed\r\n".encode())

    def cmd_LOGOUT(self, tag: str, rest: str):
        self._send(b"* BYE Logging out\r\n")
        self._send(f"{tag} OK LOGOUT completed\r\n".encode())
        raise _StopConnection()

    # -- helpers ---------------------------------------------------------

    def _expand_seqset(self, seqset: str):
        ids = sorted(self.mailbox.messages)
        if not seqset:
            return ids
        result = []
        for part in seqset.split(","):
            if ":" in part:
                a, b = part.split(":")
                a = int(a)
                b = max(ids) if b == "*" else int(b)
                result.extend(i for i in ids if a <= i <= b)
            else:
                result.append(int(part))
        return result

    def _send(self, data: bytes):
        self.wfile.write(data)
        self.wfile.flush()


class MockIMAPServer(socketserver.TCPServer):
    """Minimal fake IMAP server for tests"""

    def __init__(
        self, host: str = "127.0.0.1", port: int = 0, test_mail: str = "test@localhost"
    ):
        self.test_mail = test_mail
        super().__init__((host, port), _IMAPHandler)
