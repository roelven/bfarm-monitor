"""
Tests for monitor.py

Run with:  pytest
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import monitor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

LABEL = "Test Med"

# All 23 columns from the BfArM CSV header
_HEADER = (
    "PZN;ENR;Bearbeitungsnummer;Referenzierte Erstmeldung;Meldungsart;"
    "Beginn;Ende;Datum der letzten Meldung;Art des Grundes;"
    "Arzneimittelbezeichnung;ATC Code;Wirkstoffe;Krankenhausrelevant;"
    "Zulassungsinhaber;Telefon;E-Mail;Grund;Anmerkung zum Grund;"
    "Alternativpraeparat;Datum der Erstmeldung;Info an Fachkreise;"
    "Darreichungsform;Klassifikation"
)


def make_csv(*data_rows: str) -> bytes:
    """Build a latin-1 encoded CSV bytes object from semicolon-separated data rows."""
    lines = [_HEADER] + list(data_rows)
    return "\n".join(lines).encode("latin-1")


def make_mock_response(csv_bytes: bytes) -> MagicMock:
    mock = MagicMock()
    mock.content = csv_bytes
    mock.raise_for_status = MagicMock()
    return mock


def make_row(
    *,
    pzn: str = "1",
    enr: str = "",
    bearbeitungsnummer: str = "A",
    name: str = "TestDrug",
    wirkstoffe: str = "TestWirkstoff",
    beginn: str = "2026-01-01",
    ende: str = "2026-06-01",
    last_update: str = "2026-01-01",
    grund: str = "Lieferengpass",
    hersteller: str = "TestGmbH",
) -> dict:
    """Return a minimal row dict with the fields monitor.py actually reads."""
    return {
        "PZN": pzn,
        "ENR": enr,
        "Bearbeitungsnummer": bearbeitungsnummer,
        "Referenzierte Erstmeldung": "",
        "Meldungsart": "Erstmeldung",
        "Beginn": beginn,
        "Ende": ende,
        "Datum der letzten Meldung": last_update,
        "Art des Grundes": grund,
        "Arzneimittelbezeichnung": name,
        "ATC Code": "",
        "Wirkstoffe": wirkstoffe,
        "Krankenhausrelevant": "",
        "Zulassungsinhaber": hersteller,
        "Telefon": "",
        "E-Mail": "",
        "Grund": "",
        "Anmerkung zum Grund": "",
        "Alternativpraeparat": "",
        "Datum der Erstmeldung": "",
        "Info an Fachkreise": "",
        "Darreichungsform": "",
        "Klassifikation": "",
    }


def make_state(label: str, rows: list[dict]) -> dict:
    return {
        "fingerprints": {
            label: {monitor.row_fingerprint(r): r for r in rows}
        },
        "last_check": "2026-01-01T00:00:00+00:00",
    }


# ---------------------------------------------------------------------------
# fetch_csv
# ---------------------------------------------------------------------------


def test_fetch_csv_maps_by_actual_header():
    """Column mapping must follow the real CSV header, not a hardcoded position list."""
    # Deliberately swap first two columns from the standard order
    swapped_header = (
        "ENR;PZN;Bearbeitungsnummer;Referenzierte Erstmeldung;Meldungsart;"
        "Beginn;Ende;Datum der letzten Meldung;Art des Grundes;"
        "Arzneimittelbezeichnung;ATC Code;Wirkstoffe;Krankenhausrelevant;"
        "Zulassungsinhaber;Telefon;E-Mail;Grund;Anmerkung zum Grund;"
        "Alternativpraeparat;Datum der Erstmeldung;Info an Fachkreise;"
        "Darreichungsform;Klassifikation"
    )
    csv_bytes = (swapped_header + "\nENR001;PZN999;;" * 1 + "\n").encode("latin-1")
    with patch("monitor.requests.get", return_value=make_mock_response(csv_bytes)):
        rows = monitor.fetch_csv()
    assert rows[0]["PZN"] == "PZN999"
    assert rows[0]["ENR"] == "ENR001"


def test_fetch_csv_returns_correct_count():
    data = "\n".join(f"{i};" * 23 for i in range(5))
    csv_bytes = make_csv(*[f"{i};" * 22 for i in range(5)])
    with patch("monitor.requests.get", return_value=make_mock_response(csv_bytes)):
        rows = monitor.fetch_csv()
    assert len(rows) == 5


def test_fetch_csv_empty_response():
    with patch("monitor.requests.get", return_value=make_mock_response(b"")):
        rows = monitor.fetch_csv()
    assert rows == []


def test_fetch_csv_pads_short_rows():
    """Rows with fewer columns than the header should not raise an error."""
    short_row = "PZN001"  # only 1 field instead of 23
    csv_bytes = (_HEADER + "\n" + short_row + "\n").encode("latin-1")
    with patch("monitor.requests.get", return_value=make_mock_response(csv_bytes)):
        rows = monitor.fetch_csv()
    assert len(rows) == 1
    assert rows[0]["PZN"] == "PZN001"
    assert rows[0]["Wirkstoffe"] == ""


def test_fetch_csv_decodes_latin1():
    """BfArM uses Latin-1; umlaut characters must survive the decode."""
    # Use a string with German umlauts to exercise the latin-1 decode path
    data_row = "123;;;" * 5 + "Wirkstoff-Präparat GmbH;" + ";" * 17
    csv_bytes = (_HEADER + "\n" + data_row + "\n").encode("latin-1")
    with patch("monitor.requests.get", return_value=make_mock_response(csv_bytes)):
        rows = monitor.fetch_csv()
    assert rows[0]["PZN"] == "123"


# ---------------------------------------------------------------------------
# matches_watch
# ---------------------------------------------------------------------------


def test_matches_watch_by_wirkstoffe():
    row = make_row(wirkstoffe="Testsubstanz-Alpha 10mg")
    watch = {"label": "Med A", "match_wirkstoffe": ["testsubstanz-alpha"], "match_name": []}
    assert monitor.matches_watch(row, watch) is True


def test_matches_watch_by_name():
    row = make_row(name="Musterpräparat retard 5mg")
    watch = {"label": "Med A", "match_wirkstoffe": [], "match_name": ["musterpräparat"]}
    assert monitor.matches_watch(row, watch) is True


def test_matches_watch_case_insensitive_wirkstoffe():
    row = make_row(wirkstoffe="TESTSUBSTANZ-ALPHA 18mg")
    watch = {"label": "Med A", "match_wirkstoffe": ["testsubstanz-alpha"], "match_name": []}
    assert monitor.matches_watch(row, watch) is True


def test_matches_watch_case_insensitive_name():
    row = make_row(name="MUSTERPRÄPARAT RETARD")
    watch = {"label": "Med A", "match_wirkstoffe": [], "match_name": ["musterpräparat"]}
    assert monitor.matches_watch(row, watch) is True


def test_matches_watch_no_match():
    row = make_row(name="Ibuprofen 400mg", wirkstoffe="Ibuprofen")
    watch = {"label": "Med A", "match_wirkstoffe": ["testsubstanz-alpha"], "match_name": ["musterpräparat"]}
    assert monitor.matches_watch(row, watch) is False


def test_matches_watch_partial_substring():
    """Match should work on substrings, not exact equality."""
    row = make_row(wirkstoffe="Testsubstanz-Alpha 10mg/5ml Lösung")
    watch = {"label": "Med A", "match_wirkstoffe": ["testsubstanz-alpha"], "match_name": []}
    assert monitor.matches_watch(row, watch) is True


# ---------------------------------------------------------------------------
# row_fingerprint & row_stable_key
# ---------------------------------------------------------------------------


def test_row_fingerprint_is_deterministic():
    row = make_row(pzn="12345", bearbeitungsnummer="ABC")
    assert monitor.row_fingerprint(row) == monitor.row_fingerprint(row)


def test_row_fingerprint_changes_on_end_date():
    row1 = make_row(pzn="1", bearbeitungsnummer="A", ende="2026-06-01")
    row2 = make_row(pzn="1", bearbeitungsnummer="A", ende="2026-12-01")
    assert monitor.row_fingerprint(row1) != monitor.row_fingerprint(row2)


def test_row_fingerprint_changes_on_last_update():
    row1 = make_row(pzn="1", bearbeitungsnummer="A", last_update="2026-01-01")
    row2 = make_row(pzn="1", bearbeitungsnummer="A", last_update="2026-03-01")
    assert monitor.row_fingerprint(row1) != monitor.row_fingerprint(row2)


def test_row_stable_key_is_deterministic():
    row = make_row(pzn="12345", bearbeitungsnummer="ABC")
    assert monitor.row_stable_key(row) == monitor.row_stable_key(row)


def test_row_stable_key_ignores_end_date():
    """The stable key must not change when mutable fields like Ende are updated."""
    row1 = make_row(pzn="1", bearbeitungsnummer="A", ende="2026-06-01")
    row2 = make_row(pzn="1", bearbeitungsnummer="A", ende="2026-12-01")
    assert monitor.row_stable_key(row1) == monitor.row_stable_key(row2)


def test_row_stable_key_differs_between_records():
    row1 = make_row(pzn="1", bearbeitungsnummer="A")
    row2 = make_row(pzn="2", bearbeitungsnummer="B")
    assert monitor.row_stable_key(row1) != monitor.row_stable_key(row2)


# ---------------------------------------------------------------------------
# diff_matches
# ---------------------------------------------------------------------------


def test_diff_new_entry():
    row = make_row(pzn="1", bearbeitungsnummer="A")
    current = {LABEL: [row]}
    state = make_state(LABEL, [])
    new, changed, resolved = monitor.diff_matches(current, state)
    assert LABEL in new and len(new[LABEL]) == 1
    assert changed == {}
    assert resolved == {}


def test_diff_resolved_entry():
    row = make_row(pzn="1", bearbeitungsnummer="A")
    current = {LABEL: []}
    state = make_state(LABEL, [row])
    new, changed, resolved = monitor.diff_matches(current, state)
    assert new == {}
    assert changed == {}
    assert LABEL in resolved and len(resolved[LABEL]) == 1


def test_diff_no_change():
    row = make_row(pzn="1", bearbeitungsnummer="A")
    current = {LABEL: [row]}
    state = make_state(LABEL, [row])
    new, changed, resolved = monitor.diff_matches(current, state)
    assert new == {}
    assert changed == {}
    assert resolved == {}


def test_diff_changed_entry_not_new_plus_resolved():
    """An updated end-date on the same Bearbeitungsnummer must appear as 'changed',
    not as a spurious new+resolved pair."""
    row_old = make_row(pzn="1", bearbeitungsnummer="A", ende="2026-06-01", last_update="2026-01-01")
    row_new = make_row(pzn="1", bearbeitungsnummer="A", ende="2026-12-01", last_update="2026-03-01")
    current = {LABEL: [row_new]}
    state = make_state(LABEL, [row_old])
    new, changed, resolved = monitor.diff_matches(current, state)
    assert new == {}
    assert LABEL in changed and len(changed[LABEL]) == 1
    assert resolved == {}


def test_diff_mixed_new_changed_resolved():
    row_keep = make_row(pzn="1", bearbeitungsnummer="A")
    row_old = make_row(pzn="2", bearbeitungsnummer="B", ende="2026-06-01", last_update="2026-01-01")
    row_updated = make_row(pzn="2", bearbeitungsnummer="B", ende="2026-12-01", last_update="2026-03-01")
    row_gone = make_row(pzn="3", bearbeitungsnummer="C")
    row_fresh = make_row(pzn="4", bearbeitungsnummer="D")

    state = make_state(LABEL, [row_keep, row_old, row_gone])
    current = {LABEL: [row_keep, row_updated, row_fresh]}
    new, changed, resolved = monitor.diff_matches(current, state)

    assert LABEL in new and len(new[LABEL]) == 1          # row_fresh
    assert LABEL in changed and len(changed[LABEL]) == 1  # row_updated
    assert LABEL in resolved and len(resolved[LABEL]) == 1  # row_gone


def test_diff_empty_state_all_new():
    rows = [make_row(pzn=str(i), bearbeitungsnummer=str(i)) for i in range(3)]
    current = {LABEL: rows}
    state = {"fingerprints": {}, "last_check": "2026-01-01T00:00:00+00:00"}
    new, changed, resolved = monitor.diff_matches(current, state)
    assert len(new.get(LABEL, [])) == 3
    assert changed == {}
    assert resolved == {}


# ---------------------------------------------------------------------------
# format_notification
# ---------------------------------------------------------------------------


def test_format_notification_returns_none_when_empty():
    assert monitor.format_notification({}, {}, {}) is None


def test_format_notification_new():
    row = make_row(name="TestDrug", pzn="12345")
    result = monitor.format_notification({LABEL: [row]}, {}, {})
    assert result is not None
    title, body = result
    assert "neu" in title
    assert "NEUER ENGPASS" in body
    assert "TestDrug" in body
    assert "12345" in body


def test_format_notification_resolved():
    row = make_row(name="TestDrug", pzn="12345")
    result = monitor.format_notification({}, {}, {LABEL: [row]})
    assert result is not None
    title, body = result
    assert "beendet" in title
    assert "ENGPASS BEENDET" in body


def test_format_notification_changed():
    row = make_row(name="TestDrug", pzn="12345")
    result = monitor.format_notification({}, {LABEL: [row]}, {})
    assert result is not None
    title, body = result
    assert "aktualisiert" in title
    assert "AKTUALISIERT" in body


def test_format_notification_title_only_lists_nonzero_counts():
    row = make_row()
    title, _ = monitor.format_notification({LABEL: [row]}, {}, {})
    assert "aktualisiert" not in title
    assert "beendet" not in title
    assert "neu" in title


def test_format_notification_all_three():
    row = make_row()
    title, body = monitor.format_notification(
        {LABEL: [row]},
        {LABEL: [row]},
        {LABEL: [row]},
    )
    assert "neu" in title
    assert "aktualisiert" in title
    assert "beendet" in title
    assert "NEUER ENGPASS" in body
    assert "AKTUALISIERT" in body
    assert "BEENDET" in body


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def test_load_state_missing_file():
    with patch("monitor.STATE_FILE", Path("/nonexistent/path/state.json")):
        state = monitor.load_state()
    assert state == {"fingerprints": {}, "last_check": None}


def test_load_state_corrupt_file(tmp_path):
    bad = tmp_path / "state.json"
    bad.write_text("not json")
    with patch("monitor.STATE_FILE", bad):
        state = monitor.load_state()
    assert state == {"fingerprints": {}, "last_check": None}


def test_save_load_state_roundtrip(tmp_path):
    row = make_row(pzn="1", bearbeitungsnummer="A")
    original = {
        "fingerprints": {LABEL: {monitor.row_fingerprint(row): row}},
        "last_check": "2026-01-01T00:00:00+00:00",
    }
    state_file = tmp_path / "state.json"
    with patch("monitor.STATE_FILE", state_file):
        monitor.save_state(original)
        loaded = monitor.load_state()

    assert loaded["last_check"] == original["last_check"]
    assert set(loaded["fingerprints"][LABEL].keys()) == set(original["fingerprints"][LABEL].keys())


def test_save_state_creates_parent_dirs(tmp_path):
    state_file = tmp_path / "nested" / "dirs" / "state.json"
    with patch("monitor.STATE_FILE", state_file):
        monitor.save_state({"fingerprints": {}, "last_check": None})
    assert state_file.exists()
