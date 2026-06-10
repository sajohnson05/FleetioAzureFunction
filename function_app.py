import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()


# ==================================================
# Configuration
# ==================================================

FLEETIO_BASE_URL = os.getenv("FLEETIO_BASE_URL", "https://secure.fleetio.com/api/v1")
FLEETIO_API_KEY = os.getenv("FLEETIO_API_KEY")
FLEETIO_ACCOUNT_TOKEN = os.getenv("FLEETIO_ACCOUNT_TOKEN")

BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "fleetio-raw")

SYNC_MODE = os.getenv("SYNC_MODE", "daily").lower()
DAILY_LOOKBACK_DAYS = int(os.getenv("DAILY_LOOKBACK_DAYS", "14"))

FUEL_HISTORY_MONTHS = int(os.getenv("FUEL_HISTORY_MONTHS", "18"))
SERVICE_HISTORY_MONTHS = int(os.getenv("SERVICE_HISTORY_MONTHS", "24"))
METER_HISTORY_MONTHS = int(os.getenv("METER_HISTORY_MONTHS", "24"))

MAX_PAGES_PER_ENDPOINT = int(os.getenv("MAX_PAGES_PER_ENDPOINT", "0"))

REQUEST_DELAY_SECONDS = 1.35


# ==================================================
# Helpers
# ==================================================

def get_headers():
    return {
        "Authorization": f"Token {FLEETIO_API_KEY}",
        "Account-Token": FLEETIO_ACCOUNT_TOKEN,
        "Accept": "application/json"
    }


def validate_config():
    missing = []

    if not FLEETIO_API_KEY:
        missing.append("FLEETIO_API_KEY")

    if not FLEETIO_ACCOUNT_TOKEN:
        missing.append("FLEETIO_ACCOUNT_TOKEN")

    if not BLOB_CONNECTION_STRING:
        missing.append("BLOB_CONNECTION_STRING")

    if missing:
        raise ValueError(f"Missing required app settings: {', '.join(missing)}")


def fleetio_get(url):
    logging.info(f"Calling Fleetio URL: {url}")

    response = requests.get(url, headers=get_headers(), timeout=120)

    if response.status_code == 429:
        logging.warning("Rate limit hit. Waiting 60 seconds.")
        time.sleep(60)
        response = requests.get(url, headers=get_headers(), timeout=120)

    response.raise_for_status()

    time.sleep(REQUEST_DELAY_SECONDS)

    return response.json()


def upload_json(blob_name, data):
    blob_service_client = BlobServiceClient.from_connection_string(
        BLOB_CONNECTION_STRING
    )

    blob_client = blob_service_client.get_blob_client(
        container=BLOB_CONTAINER,
        blob=blob_name
    )

    payload = json.dumps(data, default=str, indent=2)

    blob_client.upload_blob(payload, overwrite=True)

    logging.info(f"Uploaded blob: {blob_name}")


def write_log(run_date, log_rows):
    blob_name = f"logs/{run_date}/fleetio_sync_log.json"
    upload_json(blob_name, log_rows)


def utc_now():
    return datetime.now(timezone.utc)

def subtract_months(dt, months):
    year = dt.year
    month = dt.month - months

    while month <= 0:
        month += 12
        year -= 1

    day = min(dt.day, 28)

    return dt.replace(year=year, month=month, day=day)


def parse_fleetio_date(value):
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(timezone.utc)

    except Exception:
        return None


def get_record_date(record, date_fields):
    for field in date_fields:
        value = record.get(field)

        parsed = parse_fleetio_date(value)

        if parsed:
            return parsed

    return None


def get_cutoff_date(months=None):
    now = datetime.now(timezone.utc)

    if SYNC_MODE == "backfill":
        return subtract_months(now, months)

    return now - timedelta(days=DAILY_LOOKBACK_DAYS)


# ==================================================
# Cursor-based endpoints
# ==================================================

def sync_cursor_endpoint(endpoint_name, endpoint_path, run_date, cutoff_date=None, date_fields=None):
    start_time = utc_now()
    records = []
    request_count = 0

    try:
        url = f"{FLEETIO_BASE_URL}/{endpoint_path}?per_page=50"

        while url:
            data = fleetio_get(url)
            request_count += 1

            page_records = data.get("records", [])

            if cutoff_date and date_fields:
                filtered_records = []

                for record in page_records:
                    record_dt = get_record_date(record, date_fields)

                    if record_dt and record_dt >= cutoff_date:
                        filtered_records.append(record)

                records.extend(filtered_records)

                page_dates = [
                    get_record_date(record, date_fields)
                    for record in page_records
                    if get_record_date(record, date_fields)
                ]

                if page_dates and max(page_dates) < cutoff_date:
                    logging.info(f"Stopping {endpoint_name}; records are older than cutoff.")
                    break
            else:
                records.extend(page_records)

            if MAX_PAGES_PER_ENDPOINT and request_count >= MAX_PAGES_PER_ENDPOINT:
                logging.warning(f"Stopping {endpoint_name} after {request_count} pages for testing.")
                break

            next_cursor = data.get("next_cursor")

            if next_cursor:
                url = (
                    f"{FLEETIO_BASE_URL}/{endpoint_path}"
                    f"?per_page=50"
                    f"&start_cursor={next_cursor}"
                )
            else:
                url = None

        blob_name = f"{endpoint_name}/{run_date}/{endpoint_name}.json"
        upload_json(blob_name, records)

        return {
            "endpoint": endpoint_name,
            "status": "success",
            "records": len(records),
            "requests": request_count,
            "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
            "sync_mode": SYNC_MODE,
            "start_time": start_time.isoformat(),
            "end_time": utc_now().isoformat(),
            "error": None
        }

    except Exception as e:
        logging.exception(f"Error syncing {endpoint_name}")

        return {
            "endpoint": endpoint_name,
            "status": "failed",
            "records": len(records),
            "requests": request_count,
            "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
            "sync_mode": SYNC_MODE,
            "start_time": start_time.isoformat(),
            "end_time": utc_now().isoformat(),
            "error": str(e)
        }


# ==================================================
# Page-based endpoints
# ==================================================

def sync_page_endpoint(endpoint_name, endpoint_path, run_date, cutoff_date=None, date_fields=None):
    start_time = utc_now()
    records = []
    page = 1
    request_count = 0

    try:
        while True:
            url = (
                f"{FLEETIO_BASE_URL}/{endpoint_path}"
                f"?page={page}"
                f"&per_page=50"
            )

            data = fleetio_get(url)
            request_count += 1

            if not data:
                break

            if cutoff_date and date_fields:
                filtered_records = []

                for record in data:
                    record_dt = get_record_date(record, date_fields)

                    if record_dt and record_dt >= cutoff_date:
                        filtered_records.append(record)

                records.extend(filtered_records)

                page_dates = [
                    get_record_date(record, date_fields)
                    for record in data
                    if get_record_date(record, date_fields)
                ]

                if page_dates and max(page_dates) < cutoff_date:
                    logging.info(f"Stopping {endpoint_name}; records are older than cutoff.")
                    break
            else:
                records.extend(data)

            if MAX_PAGES_PER_ENDPOINT and request_count >= MAX_PAGES_PER_ENDPOINT:
                logging.warning(f"Stopping {endpoint_name} after {request_count} pages for testing.")
                break

            page += 1

        blob_name = f"{endpoint_name}/{run_date}/{endpoint_name}.json"
        upload_json(blob_name, records)

        return {
            "endpoint": endpoint_name,
            "status": "success",
            "records": len(records),
            "requests": request_count,
            "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
            "sync_mode": SYNC_MODE,
            "start_time": start_time.isoformat(),
            "end_time": utc_now().isoformat(),
            "error": None
        }

    except Exception as e:
        logging.exception(f"Error syncing {endpoint_name}")

        return {
            "endpoint": endpoint_name,
            "status": "failed",
            "records": len(records),
            "requests": request_count,
            "cutoff_date": cutoff_date.isoformat() if cutoff_date else None,
            "sync_mode": SYNC_MODE,
            "start_time": start_time.isoformat(),
            "end_time": utc_now().isoformat(),
            "error": str(e)
        }


# ==================================================
# Individual endpoint syncs
# ==================================================

def sync_vehicles(run_date):
    return sync_cursor_endpoint(
        endpoint_name="vehicles",
        endpoint_path="vehicles",
        run_date=run_date
    )


def sync_fuel_entries(run_date):
    return sync_cursor_endpoint(
        endpoint_name="fuel_entries",
        endpoint_path="fuel_entries",
        run_date=run_date,
        cutoff_date=get_cutoff_date(FUEL_HISTORY_MONTHS),
        date_fields=["date", "updated_at", "created_at"]
    )


def sync_meter_entries(run_date):
    return sync_cursor_endpoint(
        endpoint_name="meter_entries",
        endpoint_path="meter_entries",
        run_date=run_date,
        cutoff_date=get_cutoff_date(METER_HISTORY_MONTHS),
        date_fields=["date", "updated_at", "created_at"]
    )


def sync_service_entries(run_date):
    return sync_page_endpoint(
        endpoint_name="service_entries",
        endpoint_path="service_entries",
        run_date=run_date,
        cutoff_date=get_cutoff_date(SERVICE_HISTORY_MONTHS),
        date_fields=["date", "completed_at", "updated_at", "created_at"]
    )


def sync_service_reminders(run_date):
    return sync_page_endpoint(
        endpoint_name="service_reminders",
        endpoint_path="service_reminders",
        run_date=run_date
    )


# ==================================================
# Timer trigger
# ==================================================

@app.schedule(
    schedule="0 0 5 * * *",
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True
)
def FleetioDailySync(mytimer: func.TimerRequest) -> None:
    logging.info("Fleetio Daily Sync started.")
    logging.info(f"Container: {BLOB_CONTAINER}")
    logging.info(f"Base URL: {FLEETIO_BASE_URL}")

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_rows = []

    try:
        validate_config()

        log_rows.append(sync_vehicles(run_date))
        log_rows.append(sync_fuel_entries(run_date))
        log_rows.append(sync_service_entries(run_date))
        log_rows.append(sync_meter_entries(run_date))
        log_rows.append(sync_service_reminders(run_date))

        write_log(run_date, log_rows)

        logging.info("Fleetio Daily Sync completed.")

    except Exception as e:
        logging.exception("Fleetio Daily Sync failed.")

        log_rows.append({
            "endpoint": "main",
            "status": "failed",
            "records": 0,
            "requests": 0,
            "start_time": utc_now().isoformat(),
            "end_time": utc_now().isoformat(),
            "error": str(e)
        })

        try:
            write_log(run_date, log_rows)
        except Exception:
            logging.exception("Failed to write error log.")