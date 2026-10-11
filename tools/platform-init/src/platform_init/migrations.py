"""Versioned, forward-only ClickHouse migrations with drift detection.

Files are named NNNN_description.sql and applied in order. Each applied version is recorded in
<database>.schema_migrations with the checksum of its source; editing an applied file is an error
(write a new migration instead).
"""

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Any, Protocol

log = logging.getLogger(__name__)

_FILENAME = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")


class MigrationDriftError(RuntimeError):
    pass


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    source: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.source.encode()).hexdigest()

    def statements(self, variables: dict[str, str]) -> list[str]:
        for key, value in variables.items():
            if "'" in value or "\\" in value:
                raise ValueError(f"unsafe characters in migration variable {key}")
        return split_statements(Template(self.source).substitute(variables))


def discover(directory: Path) -> list[Migration]:
    migrations = []
    for path in sorted(directory.glob("*.sql")):
        match = _FILENAME.match(path.name)
        if match is None:
            raise ValueError(f"migration file {path.name} must be named NNNN_description.sql")
        migrations.append(
            Migration(int(match.group(1)), match.group(2), path.read_text(encoding="utf-8"))
        )
    versions = [m.version for m in migrations]
    if len(versions) != len(set(versions)):
        raise ValueError("duplicate migration versions")
    return migrations


def split_statements(sql: str) -> list[str]:
    """Split on semicolons that end a line. Leading comment lines are dropped."""
    statements: list[str] = []
    current: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not current and (not stripped or stripped.startswith("--")):
            continue
        current.append(line)
        if not stripped.startswith("--") and stripped.endswith(";"):
            statements.append("\n".join(current).strip().removesuffix(";").rstrip())
            current = []
    if any(line.strip() and not line.strip().startswith("--") for line in current):
        statements.append("\n".join(current).strip())
    return statements


class ClickHouse(Protocol):
    def command(self, cmd: str) -> Any: ...

    def query(self, query: str) -> Any: ...

    def insert(self, table: str, data: list[list[Any]], column_names: list[str]) -> Any: ...


def apply(
    client: ClickHouse, database: str, migrations: list[Migration], variables: dict[str, str]
) -> list[int]:
    """Apply pending migrations; returns the versions applied in this run."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise ValueError(f"invalid database name {database!r}")
    client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.schema_migrations
        (
            version     UInt32,
            name        String,
            checksum    String,
            applied_at  DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = MergeTree
        ORDER BY version
        """
    )
    applied = {
        int(version): checksum
        for version, checksum in client.query(
            f"SELECT version, checksum FROM {database}.schema_migrations"  # noqa: S608 - validated name
        ).result_rows
    }

    newly_applied = []
    for migration in migrations:
        if migration.version in applied:
            if applied[migration.version] != migration.checksum:
                raise MigrationDriftError(
                    f"migration {migration.version:04d}_{migration.name} was edited after it ran"
                )
            continue
        log.info("applying migration %04d_%s", migration.version, migration.name)
        for statement in migration.statements(variables):
            client.command(statement)
        client.insert(
            f"{database}.schema_migrations",
            [[migration.version, migration.name, migration.checksum]],
            column_names=["version", "name", "checksum"],
        )
        newly_applied.append(migration.version)
    return newly_applied
