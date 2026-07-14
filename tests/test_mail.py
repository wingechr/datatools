"""TODO"""

from threading import Thread
from time import sleep
from unittest import TestCase

from datatools.storage.mail import MailAttachmentStorageHandler
from datatools.storage.memory import MemoryDataStorage
from datatools.utils import get_free_port
from tests.mock.imap import (
    TEST_DATE,
    TEST_HOST,
    TEST_LOGIN,
    TEST_MAIL_ORIGINAL,
    TEST_MAIL_WHILTELISTED,
    MockIMAPServer,
)


class TestMail(TestCase):
    """TODO"""

    def test_mail(self):
        """TODO"""
        port = get_free_port()
        server = MockIMAPServer(host=TEST_HOST, port=port)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        sleep(1)
        storage = MemoryDataStorage()
        handler = MailAttachmentStorageHandler(
            storage=storage,
            login_mail=TEST_LOGIN,
            imap_port=port,
            email_whitelist=[TEST_MAIL_WHILTELISTED],
            use_ssl=False,
            use_starttls=False,
        )
        handler.check()
        resources = set(storage.find())
        msg_id = 1
        expected_name = f"{TEST_DATE}_{TEST_MAIL_ORIGINAL}_{msg_id}/test.txt"
        self.assertEqual({expected_name}, resources)
