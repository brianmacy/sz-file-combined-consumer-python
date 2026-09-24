"""Record parsing, Senzing error classification, and redo-record logging ids."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from senzing import SzBadInputError, SzError, SzRetryTimeoutExceededError


@dataclass(frozen=True)
class RecordInfo:
    """DATA_SOURCE / RECORD_ID extracted from a record body."""

    data_source: str
    record_id: str

    @staticmethod
    def empty() -> RecordInfo:
        """Sentinel for outcomes that belong to no record (e.g. a fatal signal)."""
        return RecordInfo("", "")


class ParseErrorKind(Enum):
    INVALID_JSON = auto()
    NOT_AN_OBJECT = auto()
    MISSING_FIELD = auto()


class ParseError(ValueError):
    """Why a body could not be turned into a well-formed :class:`RecordInfo`.

    Every kind is REJECTED (written to the reject file), never fatal: one
    poison line must not abort a multi-million-line load.
    """

    def __init__(self, kind: ParseErrorKind, field: str | None = None) -> None:
        self.kind = kind
        self.field = field
        super().__init__(self._message())

    def _message(self) -> str:
        match self.kind:
            case ParseErrorKind.INVALID_JSON:
                text = "record body is not valid JSON"
            case ParseErrorKind.NOT_AN_OBJECT:
                text = "record body is not a JSON object"
            case ParseErrorKind.MISSING_FIELD:
                text = f"record is missing required string field '{self.field}'"
        return text

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, ParseError) and other.kind == self.kind and other.field == self.field
        )

    def __hash__(self) -> int:
        return hash((self.kind, self.field))


def parse_record(body: bytes | str) -> RecordInfo:
    """Extract DATA_SOURCE and RECORD_ID from a JSON record body.

    Raises :class:`ParseError` for unparseable JSON (including non-UTF-8
    bytes), a non-object, or a missing/non-string required field.
    """
    try:
        value: Any = json.loads(body)
    except (ValueError, UnicodeDecodeError) as err:
        raise ParseError(ParseErrorKind.INVALID_JSON) from err
    if not isinstance(value, dict):
        raise ParseError(ParseErrorKind.NOT_AN_OBJECT)

    def get(key: str) -> str:
        field = value.get(key)
        if not isinstance(field, str):
            raise ParseError(ParseErrorKind.MISSING_FIELD, key)
        return field

    return RecordInfo(data_source=get("DATA_SOURCE"), record_id=get("RECORD_ID"))


class ErrorClass(Enum):
    """How an engine error should be handled."""

    BAD_INPUT_OR_TIMEOUT = auto()
    """Bad data, retry timeout (SENZ0010) or SENZ0082 -> reject / drop, keep going."""

    FATAL = auto()
    """Anything else (incl. DB connection lost / transient) -> graceful shutdown."""


SENZ_DQM_ERROR_CODE = 82
"""EAS_ERR_ERROR_WHEN_RUNNING_DQM: a data-quality plugin error (e.g. name ``**``).

The Python SDK maps native code 82 to the base ``SzError`` (no category), so
neither ``SzBadInputError`` nor ``SzRetryTimeoutExceededError`` catches it; it
is matched on the structured leading ``SENZ0082|`` code token instead.
"""

_SENZ_CODE = re.compile(r"^SENZ(\d+)\|")


def senz_error_code(err: BaseException) -> int | None:
    """The leading ``SENZnnnn|`` code of a Senzing error message, or ``None``.

    Only the leading token is considered — never a substring search anywhere
    in the message — so an unrelated message that merely mentions a code is
    not misclassified.
    """
    m = _SENZ_CODE.match(str(err))
    return int(m.group(1)) if m else None


def classify_error(err: BaseException) -> ErrorClass:
    """Classify an engine error (same policy as the Rust driver).

    * ``SzBadInputError`` family (incl. NotFound / UnknownDataSource) and
      ``SzRetryTimeoutExceededError`` (SENZ0010) -> reject/drop, keep going.
    * ``SENZ0082`` (maps to bare ``SzError``) -> reject/drop, keep going.
    * Everything else -> FATAL. This deliberately EXCLUDES the wider
      ``SzRetryableError`` family (DB connection lost / DB transient): those
      mean the DATABASE is unhealthy, not the record. Shutting down leaves the
      file resumable; rejecting would shovel good records into the reject file
      for the whole outage.
    """
    match err:
        case SzBadInputError() | SzRetryTimeoutExceededError():
            cls = ErrorClass.BAD_INPUT_OR_TIMEOUT
        case SzError() if senz_error_code(err) == SENZ_DQM_ERROR_CODE:
            cls = ErrorClass.BAD_INPUT_OR_TIMEOUT
        case _:
            cls = ErrorClass.FATAL
    return cls


def logging_id(record: str) -> str:
    """Human-readable id for a redo record: ``DS : ID``, UMF_PROC repair, or a constant."""
    try:
        value = json.loads(record)
    except ValueError:
        return "UNKNOWN RECORD"
    if not isinstance(value, dict):
        return "UNKNOWN RECORD"

    dsrc = value.get("DATA_SOURCE")
    rec_id = value.get("RECORD_ID")
    if isinstance(dsrc, str) and isinstance(rec_id, str):
        return f"{dsrc} : {rec_id}"

    umf_proc = value.get("UMF_PROC")
    if umf_proc is not None:
        try:
            param_value = umf_proc["PARAMS"][0]["PARAM"]["VALUE"]
        except (KeyError, IndexError, TypeError):
            return "UMF_PROC : REPAIR_ENTITY"
        return f"{param_value} : REPAIR_ENTITY"

    return "UNKNOWN RECORD"
