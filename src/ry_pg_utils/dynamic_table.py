import typing as T
from datetime import datetime, timezone

from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message
from ryutils import log
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    func,
    insert,
    inspect,
    types,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from ry_pg_utils.connect import ENGINE, ManagedSession, get_table_name, init_engine

FIELD_TYPE_MAP: T.Dict[int, str] = {
    FieldDescriptor.TYPE_DOUBLE: "float",
    FieldDescriptor.TYPE_FLOAT: "float",
    FieldDescriptor.TYPE_INT64: "int",
    FieldDescriptor.TYPE_UINT64: "int",
    FieldDescriptor.TYPE_INT32: "int",
    FieldDescriptor.TYPE_FIXED64: "int",
    FieldDescriptor.TYPE_FIXED32: "int",
    FieldDescriptor.TYPE_BOOL: "bool",
    FieldDescriptor.TYPE_STRING: "string",
    FieldDescriptor.TYPE_GROUP: "string",
    FieldDescriptor.TYPE_MESSAGE: "message",
    FieldDescriptor.TYPE_BYTES: "binary",
    FieldDescriptor.TYPE_UINT32: "int",
    FieldDescriptor.TYPE_ENUM: "int",
    FieldDescriptor.TYPE_SFIXED32: "int",
    FieldDescriptor.TYPE_SFIXED64: "int",
    FieldDescriptor.TYPE_SINT32: "int",
    FieldDescriptor.TYPE_SINT64: "int",
}


def _get_field_types(message_class: T.Type) -> T.Dict[str, str]:
    """Retrieve the types of each field in a Protobuf message class."""
    return {field.name: FIELD_TYPE_MAP[field.type] for field in message_class.DESCRIPTOR.fields}


def _combine_pb_timestamp(seconds: int, nanos: int) -> datetime:
    """Combines Protobuf seconds and nanos into a single datetime object."""
    total_seconds = seconds + nanos / 1e9
    return datetime.fromtimestamp(total_seconds, tz=timezone.utc)


def _create_dynamic_table(channel_name: str, pb_message: T.Type, db_name: str) -> Table:
    engine: T.Optional[Engine] = ENGINE.get(db_name)
    if not engine:
        raise ValueError(f"Database {db_name!r} not initialized")
    if not pb_message:
        raise ValueError(f"Protobuf message {pb_message!r} invalid")

    # attempt inspect
    try:
        with engine.connect() as conn:
            inspector = inspect(conn)
            existing = inspector.get_table_names()
    except OperationalError:
        engine.dispose()
        engine = init_engine(str(engine.url), db_name)
        with engine.connect() as conn:
            inspector = inspect(conn)
            existing = inspector.get_table_names()

    tbl_name = get_table_name(channel_name)
    metadata = MetaData()
    if tbl_name in existing:
        return Table(tbl_name, metadata, autoload_with=engine)

    cols: T.List[Column] = [
        Column("key", Integer, primary_key=True, autoincrement=True),
        Column("created_at", DateTime, server_default=func.now()),  # pylint: disable=not-callable
    ]
    for name, ftype in _get_field_types(pb_message).items():
        if ftype == "string":
            cols.append(Column(name, String))
        elif ftype == "int":
            cols.append(Column(name, Integer))
        elif ftype == "binary":
            cols.append(Column(name, LargeBinary))
        elif ftype == "bool":
            cols.append(Column(name, types.Boolean))
        elif ftype == "float":
            cols.append(Column(name, types.Float))
        elif ftype == "message":
            cols.append(
                Column(
                    name,
                    types.DateTime,
                    server_default=func.now(),  # pylint: disable=not-callable
                )
            )
        else:
            raise ValueError(f"Unsupported field type: {ftype!r}")

    tbl = Table(tbl_name, metadata, *cols, extend_existing=True)
    metadata.create_all(engine)
    return tbl


class DynamicTableDb:
    def __init__(self, db_name: str) -> None:
        self.db_name = db_name

    @staticmethod
    def is_in_db(msg: Message, db_name: str, channel: str, attr: str, value: T.Any) -> bool:
        """Static entry-point for existence check."""
        return DynamicTableDb(db_name).inst_is_in_db(msg, channel, attr, value)

    def inst_is_in_db(
        self,
        message_pb: Message,
        channel_name: str,
        attr: str,
        value: T.Any,
    ) -> bool:
        try:
            tbl = _create_dynamic_table(channel_name, type(message_pb), self.db_name)
        except ValueError as e:
            print(f"is_in_db error: {e}")  # explicitly use print() here to avoid circular call
            return False
        if not hasattr(tbl.c, attr):
            raise ValueError(
                f"Column '{attr}' missing in '{channel_name}', cols={list(tbl.c.keys())}"
            )
        with ManagedSession(db=self.db_name) as sess:
            if sess is None:
                return False
            stmt = tbl.select().where(getattr(tbl.c, attr) == value)
            return bool(sess.execute(stmt).fetchone())

    @staticmethod
    def log_data_to_db(
        msg: Message,
        db_name: str,
        channel: str,
        log_print_failure: bool = True,
    ) -> None:
        """Static entry-point for logging messages."""
        assert db_name.strip(), "db_name is required"
        DynamicTableDb(db_name).add_message(channel, msg, log_print_failure)

    def add_message(
        self,
        channel_name: str,
        message_pb: Message,
        log_print_failure: bool = True,
        verbose: bool = False,
    ) -> None:
        printer = log.print_fail if log_print_failure else print
        try:
            tbl = _create_dynamic_table(channel_name, type(message_pb), self.db_name)
        except ValueError as e:
            if verbose:
                printer(f"add_message table error: {e}")
            return
        data = self.protobuf_to_dict(message_pb)
        with ManagedSession(db=self.db_name) as sess:
            if sess is None:
                return
            stmt = insert(tbl).values(**data)
            try:
                sess.execute(stmt)
            except Exception as e:  # pylint: disable=broad-exception-caught
                if verbose:
                    printer(f"insert failed: {e}")

    def protobuf_to_dict(self, message_pb: Message) -> T.Dict[str, T.Any]:
        out: T.Dict[str, T.Any] = {}
        for fld in message_pb.DESCRIPTOR.fields:
            val = getattr(message_pb, fld.name)
            if fld.type == FieldDescriptor.TYPE_MESSAGE and fld.message_type.name == "Timestamp":
                out[fld.name] = _combine_pb_timestamp(val.seconds, val.nanos)
            else:
                out[fld.name] = val
        return out
