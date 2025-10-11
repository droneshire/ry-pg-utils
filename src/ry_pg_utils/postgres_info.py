class PostgresInfo:
    """Class to store the Postgres database information."""

    def __init__(self, db_name: str, user: str, password: str, host: str, port: int) -> None:
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    @staticmethod
    def null() -> "PostgresInfo":
        """Return a null PostgresInfo object."""
        return PostgresInfo("", "", "", "", 0)

    def is_null(self) -> bool:
        """Return if the PostgresInfo object is null."""
        if bool(self == PostgresInfo.null()):
            return True
        if not self.db_name.strip() or not self.host.strip() or not self.user.strip():
            return True
        if self.port == 0:
            return True
        return False

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PostgresInfo):
            return False

        return bool(
            self.db_name == other.db_name
            and self.user == other.user
            and self.password == other.password
            and self.host == other.host
            and self.port == other.port
        )

    def __str__(self) -> str:
        values = [
            f"{key}={'*' * 8 if key == 'password' else value}"
            for key, value in self.__dict__.items()
        ]
        values_string = "\n\t".join(values)
        return f"PostgresInfo({values_string})"

    def __repr__(self) -> str:
        return self.__str__()
