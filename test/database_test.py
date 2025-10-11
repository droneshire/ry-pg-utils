import datetime
import typing as T
from test.container_test_base import PostgresOnlyTestBase

import psycopg2
from config import constants
from database.author_db import AuthorDb
from database.connect import close_engine, init_database
from database.discord_message_db import DiscordMessageDb
from database.models.alert_timing import AlertTimingSchema
from database.models.message import MediaSource, Message, MessageSchema
from google.protobuf.timestamp_pb2 import Timestamp  # pylint: disable=no-name-in-module
from pb_types.discord_pb2 import MessageDataPb  # pylint: disable=no-name-in-module
from pb_types.message_pb2 import MediaMessagePb, MediaSourcePb  # pylint: disable=no-name-in-module


class DatabaseTest(PostgresOnlyTestBase):
    cursor: T.Optional[psycopg2.extensions.cursor] = None
    connection: T.Optional[psycopg2.extensions.connection] = None
    db_name: str = ""
    host: str = ""
    port: int = 0
    user: str = ""
    password: str = ""

    def set_up_db(self) -> None:
        # Get connection parameters from the parent container test base class
        params = self.get_postgres_connection_params()
        self.host = params["host"]
        self.port = params["port"]
        self.user = params["user"]
        self.password = params["password"]
        self.db_name = "test_db"

        # Connect to the default 'postgres' database first to create test database
        self.connection = psycopg2.connect(
            dbname=constants.POSTGRES_DEFAULT_DB,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

        # Drop the test database if it exists from a previous run
        try:
            self.cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name};")
        except Exception:  # pylint: disable=broad-exception-caught
            pass

        # Create the test database
        try:
            query = f'CREATE DATABASE "{self.db_name}";'
            self.cursor.execute(query)
        except psycopg2.errors.DuplicateDatabase:  # pylint: disable=no-member
            pass

        # Initialize the test database
        init_database(
            db_host=self.host,
            db_port=self.port,
            db_name=self.db_name,
            db_user=self.user,
            db_password=self.password,
        )

    def tear_down_db(self) -> None:
        # Close engine and all connections to the test database
        close_engine(self.db_name)

        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

        # Reconnect to the default 'postgres' database to drop the test database
        self.connection = psycopg2.connect(
            dbname=constants.POSTGRES_DEFAULT_DB,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

        # Drop the test database
        self.cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name};")
        self.cursor.close()
        self.connection.close()

    def test_source_enum_sync(self) -> None:
        """Test that the source enums for PB and in Python stay in sync"""
        pb_enum_names_set = set(enum.name for enum in MediaSourcePb.DESCRIPTOR.values)
        python_enum_names_set = set(enum.name for enum in MediaSource)
        missing_in_pb = python_enum_names_set - pb_enum_names_set
        missing_in_python = pb_enum_names_set - python_enum_names_set
        self.assertEqual(missing_in_pb, set())
        self.assertEqual(missing_in_python, set())

    def test_message_schema(self) -> None:
        """Test that the message schema can be created"""
        schema = MessageSchema()
        self.assertIsNotNone(schema)

        message: Message = schema.make_message(
            {
                "id": 1,
                "source": "DISCORD",
                "author": "test",
                "group": "test",
                "content": "test",
                "channel_id": "test",
                "message_metadata": "test",
            }
        )
        self.assertEqual(message.id, 1)
        self.assertEqual(message.source, "DISCORD")
        self.assertEqual(message.author, "test")
        self.assertEqual(message.group, "test")
        self.assertEqual(message.content, "test")
        self.assertEqual(message.channel_id, "test")
        self.assertEqual(message.message_metadata, "test")

    def test_source_pb_setter(self) -> None:
        """Test the source_pb setter"""
        message = Message()
        message.source_pb = MediaSourcePb.DISCORD  # type: ignore
        self.assertEqual(message.source, "DISCORD")

    def source_pb(self) -> None:
        """Test the source_pb property"""
        message = Message()
        message.source = "DISCORD"  # type: ignore
        source_pb = message.source_pb
        self.assertEqual(source_pb, MediaSourcePb.DISCORD)

    def test_utime_pb(self) -> None:
        """Test the utime_pb property"""
        message = Message()
        message.utime = datetime.datetime(2022, 1, 1, 0, 0, 0)  # type: ignore
        timestamp = message.utime_pb
        self.assertEqual(timestamp.seconds, 1640995200)

    def test_utime_pb_setter(self) -> None:
        """Test the utime_pb setter"""
        message = Message()
        timestamp = Timestamp()
        timestamp.seconds = 1640995200
        message.utime_pb = timestamp
        self.assertEqual(message.utime, datetime.datetime(2022, 1, 1, 0, 0, 0))

    def test_mtime_pb(self) -> None:
        """Test the mtime_pb property"""
        message = Message()
        message.mtime = datetime.datetime(2022, 1, 1, 0, 0, 0)  # type: ignore
        timestamp = message.mtime_pb
        self.assertEqual(timestamp.seconds, 1640995200)

    def test_ltime_pb(self) -> None:
        """Test the ltime_pb property"""
        message = Message()
        message.ltime = datetime.datetime(2022, 1, 1, 0, 0, 0)  # type: ignore
        timestamp = message.ltime_pb
        self.assertEqual(timestamp.seconds, 1640995200)

    def test_ltime_pb_setter(self) -> None:
        """Test the ltime_pb setter"""
        message = Message()
        timestamp = Timestamp()
        timestamp.seconds = 1640995200
        message.ltime_pb = timestamp
        self.assertEqual(message.ltime, datetime.datetime(2022, 1, 1, 0, 0, 0))

    def test_is_message_in_db(self) -> None:
        """Test the is_message_in_db method"""
        self.set_up_db()
        try:
            message_pb = MessageDataPb()
            message_pb.id = "id1"
            message_pb.source_user_id = "source1"

            local_id1 = "local1"
            db = DiscordMessageDb(local_id=local_id1, db=self.db_name)
            db.add_message(message_pb)
            self.assertTrue(db.is_message_in_db(message_pb, local_ids=local_id1))

            local_id2 = "local2"
            self.assertFalse(db.is_message_in_db(message_pb, local_ids=local_id2))
            self.assertTrue(db.is_message_in_db(message_pb))
            db.add_message(message_pb, local_id=local_id2)
            self.assertTrue(db.is_message_in_db(message_pb, local_ids=local_id2))
        except AssertionError as e:
            print(f"AssertionError: {e}")
            raise
        finally:
            self.tear_down_db()

    def test_is_message_in_db_multi_local_id(self) -> None:
        """Test the is_message_in_db method using multiple local_ids"""
        self.set_up_db()
        try:
            message_pb = MessageDataPb()
            message_pb.id = "id1"
            message_pb.source_user_id = "source1"

            local_id1 = "local1"
            local_id2 = "local2"
            local_ids = [local_id1, local_id2]

            db = DiscordMessageDb(local_id=local_id1, db=self.db_name)
            db.add_message(message_pb)
            self.assertTrue(db.is_message_in_db(message_pb, local_ids=local_ids))
            self.assertTrue(db.is_message_in_db(message_pb, local_ids=None))
        except AssertionError as e:
            print(f"AssertionError: {e}")
            raise
        finally:
            self.tear_down_db()

    def test_add_alert_timing(self) -> None:
        self.set_up_db()

        message_id1 = "message_id1"
        alert_id1 = "alert_id1"
        alert_id2 = "alert_id2"
        alert_source1 = "alert_source1"
        user_id = "user_id1"
        message_pb = MediaMessagePb()
        message_pb.id = message_id1
        timestamp = Timestamp()

        try:
            db = AuthorDb(db=self.db_name)
            db.add_message(message_pb)
            timestamp.GetCurrentTime()
            db.add_alert_timing(alert_source1, timestamp, message_id1, alert_id1, user_id)
            alert_timings: T.List[AlertTimingSchema] = db.get_alert_timing(message_id1)
            self.assertEqual(len(alert_timings), 1)
            alert_timing = alert_timings[0]
            self.assertEqual(alert_timing["message_id"], message_id1)  # type: ignore
            self.assertEqual(alert_timing["alert_id"], alert_id1)  # type: ignore
            timestamp.GetCurrentTime()
            db.add_alert_timing(alert_source1, timestamp, message_id1, alert_id2, user_id)
            alert_timings = db.get_alert_timing(message_id1)
            self.assertEqual(len(alert_timings), 2)
            alert_timing = alert_timings[1]
            self.assertEqual(alert_timing["alert_id"], alert_id2)  # type: ignore
        except AssertionError as e:
            print(f"AssertionError: {e}")
            raise
        finally:
            self.tear_down_db()

    def test_get_context_begin_message(self) -> None:
        self.set_up_db()

        num_messages = 10
        main_message_id = 0
        message_author = "author"
        content = "content"

        # create the main message
        message_pb = MediaMessagePb()
        message_pb.id = str(main_message_id)
        message_pb.author = "author"
        message_pb.content = f"{content}{main_message_id}"
        message_pb.source_user_id = "source_user_id"

        try:
            db = AuthorDb(db=self.db_name)
            db.add_message(message_pb)

            message_pb_list = []
            for i in range(1, num_messages + 1):
                message_pb = MediaMessagePb()
                message_pb.id = str(main_message_id + i)
                message_pb.author = message_author
                message_pb.content = f"{content}{i}"
                message_pb.source_user_id = "source_user_id" if i % 2 == 0 else "source_user_id2"
                message_pb_list.append(message_pb)

            db.add_context_messages(
                message_id=str(main_message_id), context_messages_pb=message_pb_list
            )

            message_index, message_and_context = db.get_context(
                message_id=str(main_message_id), author=message_author
            )

            self.assertEqual(message_index, main_message_id)
            self.assertEqual(len(message_and_context), num_messages + 1)
            self.assertEqual(message_and_context[main_message_id], f"{content}{main_message_id}")
            for i in range(1, num_messages + 1):
                self.assertEqual(message_and_context[i], f"{content}{i}")
        except AssertionError as e:
            print(f"AssertionError: {e}")
            raise
        finally:
            self.tear_down_db()

    def test_get_context_middle_message(self) -> None:
        self.set_up_db()

        num_messages = 10
        main_message_id = num_messages // 2
        message_author = "author"
        content = "content"

        # create the main message
        message_pb = MediaMessagePb()
        message_pb.id = str(main_message_id)
        message_pb.author = "author"
        message_pb.content = f"{content}{main_message_id}"
        message_pb.source_user_id = "source_user_id"

        try:
            db = AuthorDb(db=self.db_name)
            db.add_message(message_pb)

            message_pb_list = []
            for i in range(main_message_id):
                message_pb = MediaMessagePb()
                message_pb.id = str(i)
                message_pb.author = message_author
                message_pb.content = f"{content}{i}"
                message_pb.source_user_id = "source_user_id" if i % 2 == 0 else "source_user_id2"
                message_pb_list.append(message_pb)

            for i in range(main_message_id + 1, num_messages + 1):
                message_pb = MediaMessagePb()
                message_pb.id = str(i)
                message_pb.author = message_author
                message_pb.content = f"{content}{i}"
                message_pb.source_user_id = "source_user_id" if i % 2 == 0 else "source_user_id2"
                message_pb_list.append(message_pb)

            db.add_context_messages(
                message_id=str(main_message_id), context_messages_pb=message_pb_list
            )

            message_index, message_and_context = db.get_context(
                message_id=str(main_message_id), author=message_author
            )

            self.assertEqual(message_index, main_message_id)
            self.assertEqual(len(message_and_context), num_messages + 1)
            self.assertEqual(message_and_context[main_message_id], f"{content}{main_message_id}")
            for i in range(main_message_id):
                self.assertEqual(message_and_context[i], f"{content}{i}")
            for i in range(main_message_id + 1, num_messages):
                self.assertEqual(message_and_context[i], f"{content}{i}")
        except AssertionError as e:
            print(f"AssertionError: {e}")
            raise
        finally:
            self.tear_down_db()

    def test_get_context_end_message(self) -> None:
        self.set_up_db()

        num_messages = 10
        main_message_id = num_messages
        message_author = "author"
        content = "content"

        # create the main message
        message_pb = MediaMessagePb()
        message_pb.id = str(main_message_id)
        message_pb.author = "author"
        message_pb.content = f"{content}{main_message_id}"
        message_pb.source_user_id = "source_user_id"

        try:
            db = AuthorDb(db=self.db_name)
            db.add_message(message_pb)

            message_pb_list = []
            for i in range(num_messages):
                message_pb = MediaMessagePb()
                message_pb.id = str(i)
                message_pb.author = message_author
                message_pb.content = f"{content}{i}"
                message_pb.source_user_id = "source_user_id" if i % 2 == 0 else "source_user_id2"
                message_pb_list.append(message_pb)

            db.add_context_messages(
                message_id=str(main_message_id), context_messages_pb=message_pb_list
            )

            message_index, message_and_context = db.get_context(
                message_id=str(main_message_id), author=message_author
            )

            self.assertEqual(message_index, main_message_id)
            self.assertEqual(len(message_and_context), num_messages + 1)
            self.assertEqual(message_and_context[main_message_id], f"{content}{main_message_id}")
            for i in range(main_message_id):
                self.assertEqual(message_and_context[i], f"{content}{i}")
        except AssertionError as e:
            print(f"AssertionError: {e}")
            raise
        finally:
            self.tear_down_db()

    def test_get_messages(self) -> None:
        """Test the get_messages method"""
        self.set_up_db()
        try:
            channel_id = "channel_id"
            local_id = "local_id"
            num_messages = 10

            db = DiscordMessageDb(local_id=local_id, db=self.db_name)

            for i in range(num_messages):
                message_pb = MessageDataPb()
                message_pb.id = f"id{i}"
                message_pb.channel_id = channel_id
                message_pb.source_user_id = f"source{i}"
                db.add_message(message_pb)

            messages = db.get_messages(
                channel_id=channel_id,
                local_id=local_id,
                limit=num_messages + 1,
                polling_db_type=False,
            )
            self.assertEqual(len(messages), num_messages)
        except AssertionError as e:
            print(f"AssertionError: {e}")
            raise
        finally:
            self.tear_down_db()

    def test_get_channels(self) -> None:
        """Test the get_channels method"""
        self.set_up_db()
        try:
            local_id = "local_id"
            num_channels = 10

            db = DiscordMessageDb(local_id=local_id, db=self.db_name)

            for i in range(num_channels):
                message_pb = MessageDataPb()
                message_pb.channel_id = f"channel_id{i}"
                db.add_message(message_pb)

            channels = db.get_channels(local_id=local_id)
            self.assertEqual(len(channels), num_channels)
        except AssertionError as e:
            print(f"AssertionError: {e}")
            raise
        finally:
            self.tear_down_db()

    def test_get_context_with_author_filter(self) -> None:
        """Test get_context with author filtering to ensure no index out of bounds issues"""
        self.set_up_db()

        main_message_id = "123"
        author1 = "author1"
        author2 = "author2"

        try:
            db = AuthorDb(db=self.db_name)

            # Create main message from author1
            main_message = MediaMessagePb()
            main_message.id = main_message_id
            main_message.author = author1
            main_message.content = "main message"
            main_message.source_user_id = "source_user"
            db.add_message(main_message)

            # Create context messages with mixed authors
            context_messages = []
            for i in range(5):
                msg = MediaMessagePb()
                msg.id = f"{i}"
                msg.author = author1 if i % 2 == 0 else author2
                msg.content = f"context message {i}"
                msg.source_user_id = "source_user"
                context_messages.append(msg)

            db.add_context_messages(
                message_id=main_message_id, context_messages_pb=context_messages
            )

            # Test getting context filtered by author1
            message_index, messages = db.get_context(message_id=main_message_id, author=author1)

            # Verify we got the right messages
            self.assertNotEqual(message_index, db.INVALID_MESSAGE_INDEX)
            self.assertTrue(0 <= message_index < len(messages))

            # Verify all messages are from author1
            for content in messages:
                self.assertFalse(":" in content)  # Messages should not contain author prefix

            # Test getting context filtered by author2
            message_index, messages = db.get_context(message_id=main_message_id, author=author2)

            # Since main message is not from author2, should get INVALID_MESSAGE_INDEX
            self.assertEqual(message_index, db.INVALID_MESSAGE_INDEX)

            # Should only get author2's messages
            for content in messages:
                self.assertFalse(":" in content)  # Messages should not contain author prefix

        finally:
            self.tear_down_db()
