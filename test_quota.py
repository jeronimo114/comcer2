"""Regression test for the Google Sheets 429 'Read requests per minute' error.

The bug: process_clients made a number of Sheets API *read* requests that grew
linearly with the number of clients in a batch (repeated worksheet-metadata
fetches and re-opening the same consecutivos spreadsheet on every iteration),
quickly exceeding the per-minute read quota and raising APIError [429].

These tests mock the gspread layer and count read requests. After the fix, the
number of read requests must stay constant regardless of how many clients are
processed.
"""

import logging

from services.excel import Client


def make_counter():
    return {
        "reads": 0,            # Sheets API read requests (the quota that errored)
        "exports": 0,          # Drive exports (a different quota; not counted here)
        "worksheet_calls": 0,  # spreadsheet.worksheet(title)
        "worksheets_calls": 0,  # spreadsheet.worksheets()
        "open_by_key_calls": 0,
    }


class FakeWorksheet:
    def __init__(self, title, ws_id, counter):
        self.title = title
        self.id = ws_id
        self._counter = counter

    def batch_update(self, *a, **k):
        return None

    def batch_clear(self, *a, **k):
        return None

    def clear(self, *a, **k):
        return None

    def row_values(self, *a, **k):
        self._counter["reads"] += 1
        return ["a", "b", "c"]

    def get_all_values(self, *a, **k):
        self._counter["reads"] += 1
        return [["h1", "h2"]]

    def insert_row(self, *a, **k):
        return None


class FakeSpreadsheet:
    def __init__(self, counter, sheet_id="MAIN"):
        self._counter = counter
        self.id = sheet_id
        self._sheets = {
            title: FakeWorksheet(title, idx, counter)
            for idx, title in enumerate(
                ["INFO", "despacho", "lIQUIDACION", "Consec", "Decomisos"]
            )
        }
        self._sheet1 = FakeWorksheet("Hoja1", 99, counter)

    def worksheet(self, title):
        self._counter["reads"] += 1
        self._counter["worksheet_calls"] += 1
        return self._sheets[title]

    def worksheets(self):
        self._counter["reads"] += 1
        self._counter["worksheets_calls"] += 1
        return list(self._sheets.values())

    @property
    def sheet1(self):
        self._counter["reads"] += 1
        return self._sheet1

    def export(self, *a, **k):
        self._counter["exports"] += 1
        return b"fake-spreadsheet-bytes"


class FakeApiClient:
    def __init__(self, counter):
        self._counter = counter

    def open_by_key(self, key):
        self._counter["reads"] += 1
        self._counter["open_by_key_calls"] += 1
        return FakeSpreadsheet(self._counter, sheet_id=key)


def build_data(clients):
    results_individuals = []
    dispatch_details = {}
    vehicles = []
    for idx, client in enumerate(clients):
        value = f"id_{idx}"
        plate = f"PL{idx}"
        results_individuals.append(
            {
                "batch": "260524-3106",
                "destination": {"value": value, "label": client},
                "consecutive": idx,
                "property": {"label": "GRANJA"},
                "ppe": 0,
                "pcc": 0,
                "pcr": 0,
                "gd": 0,
                "ml": 0,
                "seurop": 0,
                "mc": 0,
                "mckg": 0,
                "indexpse": 0,
            }
        )
        dispatch_details[value] = {"name": client, "plate": plate, "code": "DE-1"}
        vehicles.append(
            {
                "plate": plate,
                "start_date": "2026-05-27 10:09:07",
                "end_date": "2026-05-27 10:31:58",
            }
        )
    results_lote = {
        "register": {"createdAt": "2026-05-27 10:09:07"},
        "weights": [{"weightdate": "2026-05-27 10:00:00"}],
        "databenefit": {"datebenefit": "2026-05-27 10:31:58"},
    }
    return results_lote, results_individuals, dispatch_details, vehicles


def run_processing(n):
    counter = make_counter()
    client_obj = Client.__new__(Client)  # bypass __init__ (no creds / network)
    client_obj.logger = logging.getLogger("test_quota")
    client_obj.logger.addHandler(logging.NullHandler())
    client_obj.spreadsheet = FakeSpreadsheet(counter)
    client_obj.sheets_api_client = FakeApiClient(counter)
    client_obj.batch = "260524-3106"
    client_obj.benefit_day = "2026-05-27 10:31:58"
    client_obj.generated_files = []
    client_obj.consecutivos = []
    # Caches used by the fixed implementation (harmless for the buggy one).
    client_obj._ws_cache = None
    client_obj._consec_spreadsheet = None
    # PDF export hits the network via httplib2; stub it out.
    client_obj.export_worksheet_pdf = lambda worksheet: b"%PDF-1.4 fake"

    clients = [f"CLIENT_{i}" for i in range(n)]
    results_lote, results_individuals, dispatch_details, vehicles = build_data(clients)
    client_obj.dispatch_details = dispatch_details
    client_obj.vehicles = vehicles

    client_obj.process_clients(clients, results_lote, results_individuals)
    return counter


def test_sheets_reads_do_not_scale_with_client_count():
    small = run_processing(3)
    big = run_processing(30)
    assert big["reads"] == small["reads"], (
        "Sheets read requests scale with client count "
        f"(3 clients -> {small['reads']} reads, 30 clients -> {big['reads']} reads). "
        "This is what triggers the 429 'Read requests per minute' quota error."
    )


def test_consecutivos_destination_opened_once_per_batch():
    counter = run_processing(20)
    assert counter["open_by_key_calls"] == 1, (
        "Consecutivos destination spreadsheet opened "
        f"{counter['open_by_key_calls']} times for 20 clients; "
        "it is client-invariant and should be opened once per batch."
    )


def test_worksheet_metadata_fetched_once_per_batch():
    counter = run_processing(20)
    total_meta = counter["worksheet_calls"] + counter["worksheets_calls"]
    assert total_meta <= 1, (
        f"Worksheet metadata fetched {total_meta} times for 20 clients; "
        "worksheet handles should be cached and fetched at most once per batch."
    )
