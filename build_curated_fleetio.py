import os
import json
import logging
from io import BytesIO
from datetime import datetime, timezone

import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


# Azure local.settings.json handled by get_setting()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


RAW_CONTAINER = os.getenv("RAW_CONTAINER", "fleetio-raw")
CURATED_CONTAINER = os.getenv("CURATED_CONTAINER", "fleetio-curated")
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")


def get_setting(name, default=None):
    value = os.getenv(name)
    if value:
        return value

    try:
        with open("local.settings.json", "r", encoding="utf-8") as f:
            settings = json.load(f)
        return settings.get("Values", {}).get(name, default)
    except FileNotFoundError:
        return default


BLOB_CONNECTION_STRING = get_setting("BLOB_CONNECTION_STRING")
RAW_CONTAINER = get_setting("RAW_CONTAINER", "fleetio-raw")
CURATED_CONTAINER = get_setting("CURATED_CONTAINER", "fleetio-curated")


def blob_service():
    if not BLOB_CONNECTION_STRING:
        raise ValueError("Missing BLOB_CONNECTION_STRING")

    return BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)


def list_blobs(container_name, prefix):
    client = blob_service().get_container_client(container_name)
    return list(client.list_blobs(name_starts_with=prefix))


def get_latest_blob(container_name, endpoint_name, file_name):
    prefix = f"{endpoint_name}/"
    blobs = list_blobs(container_name, prefix)

    matching = [
        b.name for b in blobs
        if b.name.endswith(f"/{file_name}")
    ]

    if not matching:
        raise FileNotFoundError(
            f"No blobs found for {endpoint_name}/{file_name}"
        )

    matching.sort(reverse=True)
    return matching[0]


def download_json(container_name, blob_name):
    client = blob_service().get_blob_client(
        container=container_name,
        blob=blob_name
    )

    raw = client.download_blob().readall()
    return json.loads(raw)


def upload_parquet(container_name, blob_name, df):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    client = blob_service().get_blob_client(
        container=container_name,
        blob=blob_name
    )

    client.upload_blob(buffer, overwrite=True)

    logging.info(
        "Uploaded %s rows to %s/%s",
        len(df),
        container_name,
        blob_name
    )


def to_decimal(value):
    if value in [None, ""]:
        return None

    try:
        return float(value)
    except Exception:
        return None


def to_dollars_from_cents(value):
    if value in [None, ""]:
        return None

    try:
        return float(value) / 100
    except Exception:
        return None


def to_bool(value):
    if value is None:
        return None

    return bool(value)


def flatten_vehicles(records):
    rows = []

    for r in records:
        driver = r.get("driver") or {}
        specs = r.get("specs") or {}

        rows.append({
            "VehicleID": r.get("id"),
            "AccountID": r.get("account_id"),
            "VehicleName": r.get("name"),
            "VIN": r.get("vin"),
            "LicensePlate": r.get("license_plate"),
            "Year": r.get("year"),
            "Make": r.get("make"),
            "Model": r.get("model"),
            "Trim": r.get("trim"),
            "Color": r.get("color"),

            "Ownership": r.get("ownership"),
            "VehicleStatusID": r.get("vehicle_status_id"),
            "VehicleStatusName": r.get("vehicle_status_name"),
            "VehicleTypeID": r.get("vehicle_type_id"),
            "VehicleTypeName": r.get("vehicle_type_name"),

            "GroupID": r.get("group_id"),
            "GroupName": r.get("group_name"),
            "GroupAncestry": r.get("group_ancestry"),

            "FuelTypeID": r.get("fuel_type_id"),
            "FuelTypeName": r.get("fuel_type_name"),
            "FuelVolumeUnits": r.get("fuel_volume_units"),

            "PrimaryMeterUnit": r.get("primary_meter_unit"),
            "PrimaryMeterValue": to_decimal(r.get("primary_meter_value")),
            "PrimaryMeterDate": r.get("primary_meter_date"),
            "PrimaryMeterUsagePerDay": to_decimal(r.get("primary_meter_usage_per_day")),

            "InServiceDate": r.get("in_service_date"),
            "InServiceMeterValue": to_decimal(r.get("in_service_meter_value")),
            "OutOfServiceDate": r.get("out_of_service_date"),
            "OutOfServiceMeterValue": to_decimal(r.get("out_of_service_meter_value")),

            "EstimatedServiceMonths": r.get("estimated_service_months"),
            "EstimatedReplacementMileage": to_decimal(r.get("estimated_replacement_mileage")),
            "EstimatedResalePrice": to_dollars_from_cents(r.get("estimated_resale_price_cents")),

            "FuelEntriesCount": r.get("fuel_entries_count"),
            "ServiceEntriesCount": r.get("service_entries_count"),
            "ServiceRemindersCount": r.get("service_reminders_count"),
            "IssuesCount": r.get("issues_count"),
            "WorkOrdersCount": r.get("work_orders_count"),

            "DriverID": driver.get("id"),
            "DriverName": driver.get("name"),
            "DriverEmail": driver.get("email"),
            "DriverEmployee": driver.get("employee"),
            "DriverEmployeeNumber": driver.get("employee_number"),
            "DriverGroupID": driver.get("group_id"),

            "BodyType": specs.get("body_type"),
            "BodySubtype": specs.get("body_subtype"),
            "DriveType": specs.get("drive_type"),
            "MSRP": specs.get("msrp"),
            "MSRPCents": specs.get("msrp_cents"),
            "FuelTankCapacity": specs.get("fuel_tank_capacity"),
            "EPACombined": specs.get("epa_combined"),
            "EPACity": specs.get("epa_city"),
            "EPAHighway": specs.get("epa_highway"),

            "ArchivedAt": r.get("archived_at"),
            "CreatedAt": r.get("created_at"),
            "UpdatedAt": r.get("updated_at"),
            "CuratedAtUTC": datetime.now(timezone.utc).isoformat()
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.drop_duplicates(subset=["VehicleID"], keep="last")

    return df


def flatten_fuel_entries(records):
    rows = []

    for r in records:
        vehicle = r.get("vehicle") or {}
        meter = r.get("meter_entry") or {}
        vendor = r.get("vendor") or {}

        rows.append({
            "FuelEntryID": r.get("id"),
            "ExternalID": r.get("external_id"),
            "VehicleID": r.get("vehicle_id"),
            "VendorID": r.get("vendor_id"),
            "FuelTypeID": r.get("fuel_type_id"),

            "FuelDate": r.get("date"),
            "CreatedAt": r.get("created_at"),
            "UpdatedAt": r.get("updated_at"),

            "Region": r.get("region"),
            "Partial": r.get("partial"),
            "Personal": r.get("personal"),
            "Reset": r.get("reset"),

            "GallonsUS": to_decimal(r.get("us_gallons")),
            "Liters": to_decimal(r.get("liters")),
            "PricePerUnit": to_decimal(r.get("price_per_volume_unit")),
            "TotalAmountCents": r.get("total_amount_cents"),
            "TotalAmount": to_dollars_from_cents(r.get("total_amount_cents")),

            "MPGUS": to_decimal(r.get("mpg_us")),
            "CostPerMile": to_decimal(r.get("cost_per_mi")),
            "UsageMiles": to_decimal(r.get("usage_in_mi")),

            "MeterEntryID": meter.get("id"),
            "MeterValue": to_decimal(meter.get("value")),
            "MeterDate": meter.get("date"),
            "MeterVoid": meter.get("void"),

            "VehicleName": vehicle.get("name"),
            "VehicleYear": vehicle.get("year"),
            "VehicleMake": vehicle.get("make"),
            "VehicleModel": vehicle.get("model"),
            "VehicleVIN": vehicle.get("vin"),
            "VehicleLicensePlate": vehicle.get("license_plate"),

            "VendorName": vendor.get("name"),
            "VendorCity": vendor.get("city"),
            "VendorRegion": vendor.get("region"),
            "VendorPostalCode": vendor.get("postal_code"),
            "VendorExternalID": vendor.get("external_id"),

            "CuratedAtUTC": datetime.now(timezone.utc).isoformat()
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.drop_duplicates(subset=["FuelEntryID"], keep="last")

    return df


def flatten_service_entries(records):
    header_rows = []
    line_rows = []

    for r in records:
        vehicle = r.get("vehicle") or {}
        vendor = r.get("vendor") or {}
        meter = r.get("meter_entry") or {}

        service_entry_id = r.get("id")
        vehicle_id = r.get("vehicle_id") or vehicle.get("id")

        header_rows.append({
            "ServiceEntryID": service_entry_id,
            "VehicleID": vehicle_id,
            "VendorID": r.get("vendor_id") or vendor.get("id"),

            "Reference": r.get("reference"),
            "ServiceDate": r.get("date"),
            "StartedAt": r.get("started_at"),
            "CompletedAt": r.get("completed_at"),
            "CreatedAt": r.get("created_at"),
            "UpdatedAt": r.get("updated_at"),

            "Status": r.get("status"),
            "Description": r.get("description"),

            "LaborSubtotal": to_decimal(r.get("labor_subtotal")),
            "PartsSubtotal": to_decimal(r.get("parts_subtotal")),
            "Fees": to_decimal(r.get("fees")),
            "Tax1": to_decimal(r.get("tax_1")),
            "Tax2": to_decimal(r.get("tax_2")),
            "Subtotal": to_decimal(r.get("subtotal")),
            "TotalAmount": to_decimal(r.get("total_amount")),

            "MeterEntryID": meter.get("id"),
            "MeterValue": to_decimal(meter.get("value")),
            "MeterDate": meter.get("date"),
            "MeterVoid": meter.get("void"),

            "VehicleName": vehicle.get("name"),
            "VehicleYear": vehicle.get("year"),
            "VehicleMake": vehicle.get("make"),
            "VehicleModel": vehicle.get("model"),
            "VehicleVIN": vehicle.get("vin"),

            "VendorName": vendor.get("name"),
            "VendorCity": vendor.get("city"),
            "VendorRegion": vendor.get("region"),

            "CuratedAtUTC": datetime.now(timezone.utc).isoformat()
        })

        line_items = r.get("service_entry_line_items") or []

        for idx, li in enumerate(line_items, start=1):
            service_task = li.get("service_task") or {}
            vmrs = li.get("vmrs") or {}

            line_rows.append({
                "ServiceEntryID": service_entry_id,
                "LineNumber": idx,
                "VehicleID": vehicle_id,

                "LineItemID": li.get("id"),
                "LineItemType": li.get("type"),
                "Description": li.get("description"),

                "ServiceTaskID": li.get("service_task_id") or service_task.get("id"),
                "ServiceTaskName": service_task.get("name"),

                "LaborCost": to_decimal(li.get("labor_cost")),
                "PartsCost": to_decimal(li.get("parts_cost")),
                "Subtotal": to_decimal(li.get("subtotal")),

                "VMRSSystemGroup": vmrs.get("system_group"),
                "VMRSSystem": vmrs.get("system"),
                "VMRSAssembly": vmrs.get("assembly"),
                "VMRSComponent": vmrs.get("component"),

                "CuratedAtUTC": datetime.now(timezone.utc).isoformat()
            })

    header_df = pd.DataFrame(header_rows)
    line_df = pd.DataFrame(line_rows)

    if not header_df.empty:
        header_df = header_df.drop_duplicates(
            subset=["ServiceEntryID"],
            keep="last"
        )

    if not line_df.empty:
        if "LineItemID" in line_df.columns and line_df["LineItemID"].notna().any():
            line_df = line_df.drop_duplicates(
                subset=["ServiceEntryID", "LineItemID"],
                keep="last"
            )
        else:
            line_df = line_df.drop_duplicates(
                subset=["ServiceEntryID", "LineNumber"],
                keep="last"
            )

    return header_df, line_df


def flatten_meter_entries(records):
    rows = []

    for r in records:
        vehicle = r.get("vehicle") or {}

        rows.append({
            "MeterEntryID": r.get("id"),
            "VehicleID": r.get("vehicle_id"),
            "MeterDate": r.get("date"),
            "MeterValue": to_decimal(r.get("value")),
            "MeterType": r.get("meter_type"),
            "MeterCategory": r.get("category"),
            "MeterableID": r.get("meterable_id"),
            "MeterableType": r.get("meterable_type"),
            "Void": r.get("void"),
            "AutoVoidedAt": r.get("auto_voided_at"),
            "CreatedAt": r.get("created_at"),
            "UpdatedAt": r.get("updated_at"),

            "VehicleName": vehicle.get("name"),
            "VehicleYear": vehicle.get("year"),
            "VehicleMake": vehicle.get("make"),
            "VehicleModel": vehicle.get("model"),
            "VehicleVIN": vehicle.get("vin"),

            "CuratedAtUTC": datetime.now(timezone.utc).isoformat()
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.drop_duplicates(subset=["MeterEntryID"], keep="last")

    return df


def flatten_service_reminders(records):
    rows = []

    for r in records:
        vehicle = r.get("vehicle") or {}
        service_task = r.get("service_task") or {}

        rows.append({
            "ServiceReminderID": r.get("id"),
            "VehicleID": r.get("vehicle_id") or vehicle.get("id"),
            "ServiceTaskID": r.get("service_task_id") or service_task.get("id"),
            "ServiceTaskName": service_task.get("name"),

            "DueSoon": r.get("due_soon"),
            "Overdue": r.get("overdue"),
            "DueStatus": r.get("due_status"),

            "MeterInterval": to_decimal(r.get("meter_interval")),
            "TimeInterval": r.get("time_interval"),
            "TimeFrequency": r.get("time_frequency"),

            "NextDueMeterValue": to_decimal(r.get("next_due_meter_value")),
            "NextDueDate": r.get("next_due_date"),

            "VehicleName": vehicle.get("name"),
            "VehicleYear": vehicle.get("year"),
            "VehicleMake": vehicle.get("make"),
            "VehicleModel": vehicle.get("model"),
            "VehicleVIN": vehicle.get("vin"),

            "CreatedAt": r.get("created_at"),
            "UpdatedAt": r.get("updated_at"),
            "CuratedAtUTC": datetime.now(timezone.utc).isoformat()
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.drop_duplicates(subset=["ServiceReminderID"], keep="last")

    return df


def process_endpoint(endpoint_name, file_name, flatten_func, output_name):
    latest_blob = get_latest_blob(
        RAW_CONTAINER,
        endpoint_name,
        file_name
    )

    logging.info("Reading latest raw blob: %s", latest_blob)

    records = download_json(RAW_CONTAINER, latest_blob)

    df = flatten_func(records)

    output_blob = f"{output_name}/{output_name}.parquet"

    upload_parquet(
        CURATED_CONTAINER,
        output_blob,
        df
    )

    return {
        "endpoint": endpoint_name,
        "source_blob": latest_blob,
        "output_blob": output_blob,
        "rows": len(df)
    }


def process_service_entries():
    latest_blob = get_latest_blob(
        RAW_CONTAINER,
        "service_entries",
        "service_entries.json"
    )

    logging.info("Reading latest raw blob: %s", latest_blob)

    records = download_json(RAW_CONTAINER, latest_blob)

    header_df, line_df = flatten_service_entries(records)

    upload_parquet(
        CURATED_CONTAINER,
        "fact_service_entry/fact_service_entry.parquet",
        header_df
    )

    upload_parquet(
        CURATED_CONTAINER,
        "fact_service_entry_line_item/fact_service_entry_line_item.parquet",
        line_df
    )

    return {
        "endpoint": "service_entries",
        "source_blob": latest_blob,
        "service_entry_rows": len(header_df),
        "service_line_rows": len(line_df)
    }


def main():
    logging.info("Fleetio curated build started")

    results = []

    results.append(
        process_endpoint(
            endpoint_name="vehicles",
            file_name="vehicles.json",
            flatten_func=flatten_vehicles,
            output_name="dim_vehicle"
        )
    )

    results.append(
        process_endpoint(
            endpoint_name="fuel_entries",
            file_name="fuel_entries.json",
            flatten_func=flatten_fuel_entries,
            output_name="fact_fuel_entry"
        )
    )

    results.append(process_service_entries())

    results.append(
        process_endpoint(
            endpoint_name="meter_entries",
            file_name="meter_entries.json",
            flatten_func=flatten_meter_entries,
            output_name="fact_meter_entry"
        )
    )

    results.append(
        process_endpoint(
            endpoint_name="service_reminders",
            file_name="service_reminders.json",
            flatten_func=flatten_service_reminders,
            output_name="fact_service_reminder"
        )
    )

    summary_df = pd.DataFrame(results)

    upload_parquet(
        CURATED_CONTAINER,
        "logs/curated_build_log.parquet",
        summary_df
    )

    logging.info("Fleetio curated build completed")
    logging.info(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()