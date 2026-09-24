from __future__ import annotations

import pytest
from senzing import (
    SzBadInputError,
    SzConfigurationError,
    SzDatabaseConnectionLostError,
    SzDatabaseError,
    SzDatabaseTransientError,
    SzError,
    SzLicenseError,
    SzNotFoundError,
    SzNotInitializedError,
    SzRetryTimeoutExceededError,
    SzUnhandledError,
    SzUnknownDataSourceError,
)

from sz_file_combined_consumer.record import (
    ErrorClass,
    ParseError,
    ParseErrorKind,
    classify_error,
    logging_id,
    parse_record,
    senz_error_code,
)


def test_parses_data_source_and_record_id() -> None:
    info = parse_record(b'{"DATA_SOURCE":"TEST","RECORD_ID":"R1","NAME_FULL":"A B"}')
    assert (info.data_source, info.record_id) == ("TEST", "R1")


def test_parses_str_body_too() -> None:
    info = parse_record('{"DATA_SOURCE":"TEST","RECORD_ID":"R2"}')
    assert info.record_id == "R2"


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (b'{"RECORD_ID":"R1"}', ParseError(ParseErrorKind.MISSING_FIELD, "DATA_SOURCE")),
        (b'{"DATA_SOURCE":"TEST"}', ParseError(ParseErrorKind.MISSING_FIELD, "RECORD_ID")),
        (
            b'{"DATA_SOURCE":123,"RECORD_ID":"R1"}',
            ParseError(ParseErrorKind.MISSING_FIELD, "DATA_SOURCE"),
        ),
        (b"not json", ParseError(ParseErrorKind.INVALID_JSON)),
        (b"\xff\xfe", ParseError(ParseErrorKind.INVALID_JSON)),
        (b"42", ParseError(ParseErrorKind.NOT_AN_OBJECT)),
        (b"[1]", ParseError(ParseErrorKind.NOT_AN_OBJECT)),
    ],
)
def test_parse_errors_are_rejects(body: bytes, expected: ParseError) -> None:
    with pytest.raises(ParseError) as exc:
        parse_record(body)
    assert exc.value == expected
    assert str(exc.value)


def test_parse_error_messages() -> None:
    assert str(ParseError(ParseErrorKind.INVALID_JSON)) == "record body is not valid JSON"
    assert str(ParseError(ParseErrorKind.NOT_AN_OBJECT)) == "record body is not a JSON object"
    assert "'RECORD_ID'" in str(ParseError(ParseErrorKind.MISSING_FIELD, "RECORD_ID"))
    assert ParseError(ParseErrorKind.INVALID_JSON) != "x"
    assert (
        len({ParseError(ParseErrorKind.INVALID_JSON), ParseError(ParseErrorKind.INVALID_JSON)}) == 1
    )


def test_senz_error_code_only_matches_leading_token() -> None:
    assert senz_error_code(SzError("SENZ0082|Error when running DQM '**'")) == 82
    assert senz_error_code(SzError("SENZ2207|Data source code [NOPE] does not exist.")) == 2207
    assert senz_error_code(SzError("something mentioning SENZ0082 later")) is None
    assert senz_error_code(SzError("")) is None


@pytest.mark.parametrize(
    "err",
    [
        SzBadInputError("SENZ0023|bad"),
        SzNotFoundError("SENZ0037|no such record"),
        SzUnknownDataSourceError("SENZ2207|Data source code [NOPE] does not exist."),
        SzRetryTimeoutExceededError("SENZ0010|retry timeout"),
        SzError("SENZ0082|Error when running DQM '**'"),
    ],
)
def test_bad_input_timeout_and_dqm_are_rejects(err: Exception) -> None:
    assert classify_error(err) is ErrorClass.BAD_INPUT_OR_TIMEOUT


def test_retry_timeout_is_not_fatal_regression_guard() -> None:
    # SENZ0010 is retryable in the SDK taxonomy; it must be rejected, never fatal.
    from senzing import SzRetryableError

    err = SzRetryTimeoutExceededError("SENZ0010|timeout")
    assert isinstance(err, SzRetryableError)
    assert classify_error(err) is not ErrorClass.FATAL


@pytest.mark.parametrize(
    "err",
    [
        SzDatabaseConnectionLostError("SENZ1001|conn lost"),
        SzDatabaseTransientError("SENZ1002|deadlock"),
        SzDatabaseError("SENZ0999|db"),
        SzConfigurationError("SENZ0007|bad config"),
        SzLicenseError("SENZ0008|expired"),
        SzNotInitializedError("SENZ0048|no init"),
        SzUnhandledError("SENZ9999|boom"),
        SzError("some unmapped internal error"),
        RuntimeError("ctypes blew up"),
    ],
)
def test_everything_else_is_fatal(err: Exception) -> None:
    # DB-connection-lost / transient are retryable in the SDK but describe an
    # unhealthy DATABASE, not a bad record: they stay FATAL so the load stops
    # and can be resumed instead of shovelling good records into the reject file.
    assert classify_error(err) is ErrorClass.FATAL


def test_logging_id_variants() -> None:
    assert logging_id('{"DATA_SOURCE":"TEST","RECORD_ID":"42"}') == "TEST : 42"
    assert logging_id('{"UMF_PROC":{"PARAMS":[{"PARAM":{"VALUE":"99"}}]}}') == "99 : REPAIR_ENTITY"
    assert logging_id('{"UMF_PROC":{"PARAMS":[]}}') == "UMF_PROC : REPAIR_ENTITY"
    assert (
        logging_id('{"UMF_PROC":{"PARAMS":[{"PARAM":{"NAME":"ENTITY_ID","VALUE":7}}]}}')
        == "7 : REPAIR_ENTITY"
    )
    assert logging_id("not json") == "UNKNOWN RECORD"
    assert logging_id("[1,2]") == "UNKNOWN RECORD"
    assert logging_id('{"OTHER":1}') == "UNKNOWN RECORD"
