import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
from psycopg2.extras import execute_values
import psycopg2
import logging
import smtplib
from email.mime.text import MIMEText
import pytz
from config import *

# --- Logging ---
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

# --- Email ---
def send_email_alert(subject, message):
    try:
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = SMTP_USER
        msg['To'] = ALERT_EMAIL
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, [ALERT_EMAIL], msg.as_string())
        server.quit()
        logging.info("Alert email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send alert email: {e}")

# --- Germany TSOs ---
germany_control_areas = {
    "TransnetBW": "10YDE-ENBW-----N",
    "TenneT": "10YDE-EON------1",
    "Amprion": "10YDE-RWENET---I",
    "50Hertz": "10YDE-VE-------2"
}
germany_bidding_zone = "10Y1001A1001A82H"  # DE-LU BZN

# --- Helper to compute interval ---
def get_time_interval(start_time, resolution, position):
    if resolution == "PT15M":
        delta = timedelta(minutes=15)
    elif resolution == "PT30M":
        delta = timedelta(minutes=30)
    else:
        delta = timedelta(hours=1)
    point_start = start_time + delta * (position - 1)
    point_end = point_start + delta
    return point_start, point_end

# --- Unified ETL function for Balancing Reserves ---
def fetch_and_store_data(country_name, control_area, start_dt, end_dt):
    try:
        # Attach UTC explicitly
        start_dt_naive = start_dt.replace(tzinfo=None)
        end_dt_naive = end_dt.replace(tzinfo=None)
        utc = pytz.utc
        start_dt = utc.localize(start_dt_naive)
        end_dt = utc.localize(end_dt_naive)
        cet = pytz.timezone("Europe/Berlin")
        start_dt_cet = start_dt.astimezone(cet)
        end_dt_cet = end_dt.astimezone(cet)
        period_start = start_dt_cet.strftime("%Y%m%d%H%M")
        period_end = end_dt_cet.strftime("%Y%m%d%H%M")
        process_types = ["A51", "A52", "A47", "A46"]
        data = []

        for process_type in process_types:
            try:
                API_URL = "https://web-api.tp.entsoe.eu/api"
                PARAMS = {
                    "securityToken": SECURITY_TOKEN,
                    "documentType": "A81",
                    "businessType": "B95",
                    "processType": process_type,
                    "Type_MarketAgreement.Type": "A01",
                    "controlArea_Domain": control_area,
                    "periodStart": period_start,
                    "periodEnd": period_end
                }
                response = requests.get(API_URL, params=PARAMS)
                response.raise_for_status()
                root = ET.fromstring(response.content)
                ns = {'ns': root.tag.split('}')[0].strip('{')}
                reserve_map = {"A51": "AFRR", "A52": "FCR", "A47": "MFRR", "A46": "RR"}
                reserve_source_map = {"A04": "Generation", "A05": "Load", "A03": "Mixed"}
                direction_map = {"A01": "Up", "A02": "Down", "A03": "Up and Down (Symmetric)"}

                for ts in root.findall(".//ns:TimeSeries", ns):
                    reserve_type = reserve_map.get(process_type, process_type)
                    reserve_source1 = ts.find("ns:mktPSRType.psrType", ns)
                    reserve_source = reserve_source_map.get(reserve_source1.text, reserve_source1.text) if reserve_source1 is not None else None
                    direction_code = ts.find("ns:flowDirection.direction", ns)
                    direction = direction_map.get(direction_code.text, direction_code.text) if direction_code is not None else None
                    product_type = "Standard"
                    time_horizon = "Daily"
                    if direction in ("Up","Down"):
                        price_type = "Average"
                    else:
                        price_type = "Marginal"
                    for period in ts.findall("ns:Period", ns):
                        start_time_str = period.find("ns:timeInterval/ns:start", ns).text
                        start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
                        resolution = period.find("ns:resolution", ns).text
                        for point in period.findall("ns:Point", ns):
                            quantity_el = point.find("ns:quantity", ns)
                            price_el = point.find("ns:procurement_Price.amount", ns)
                            quantity = float(quantity_el.text) if quantity_el is not None else None
                            price = float(price_el.text) if price_el is not None else None
                            position_el = point.find("ns:position", ns)
                            position = int(position_el.text) if position_el is not None else 1
                            point_start, point_end = get_time_interval(start_time, resolution, position)
                            delivery_period = f"{point_start.strftime('%d.%m.%Y %H:%M')} - {point_end.strftime('%d.%m.%Y %H:%M')} (UTC)"
                            data.append({
                                "delivery_period": delivery_period,
                                "reserve_type": reserve_type,
                                "reserve_source": reserve_source,
                                "direction": direction,
                                "volume": quantity,
                                "price": price,
                                "price_type": price_type,
                                "type_of_product": product_type,
                                "time_horizon": time_horizon,
                                "country": country_name,
                                "control_area": control_area
                            })
            except Exception as inner_e:
                logging.error(f"Failed to fetch {process_type}: {inner_e}", exc_info=True)

        if not data:
            logging.warning(f"No data for {country_name} {control_area} {period_start} - {period_end}")
            return

        df = pd.DataFrame(data)
        conn = psycopg2.connect(dbname=AZURE_PG_DB, user=AZURE_PG_USER, password=AZURE_PG_PASSWORD,
                                host=AZURE_PG_HOST, port=5432, sslmode='require')
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS entsoe_load_data (
            delivery_period TEXT,
            reserve_type TEXT,
            reserve_source TEXT,
            direction TEXT,
            volume DOUBLE PRECISION,
            price DOUBLE PRECISION,
            price_type TEXT,
            type_of_product TEXT,
            time_horizon TEXT
        )
        """)
        conn.commit()

        # --- Add missing columns ---
        required_columns = {"country": "TEXT", "control_area": "TEXT", "inserted_at": "TIMESTAMP DEFAULT now()"}
        for col, col_type in required_columns.items():
            cursor.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='entsoe_load_data' AND column_name='{col}') THEN
                    ALTER TABLE entsoe_load_data ADD COLUMN {col} {col_type};
                END IF;
            END
            $$;
            """)
        conn.commit()

        records = df.to_dict("records")
        columns = df.columns.tolist()
        values = [[r.get(col) for col in columns] for r in records]
        execute_values(cursor,
                       f"INSERT INTO entsoe_load_data ({', '.join(columns)}) VALUES %s",
                       values)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Inserted {len(df)} rows for {country_name} {period_start} - {period_end}")
    except Exception as e:
        logging.error(f"ETL failed for {country_name} {period_start} - {period_end}: {e}", exc_info=True)
        send_email_alert("ENTSOE ETL Failed", f"{country_name} {period_start}-{period_end}\n{e}")
        raise

# --- Unified ETL function for Day-ahead ---
def fetch_and_store_dayahead_prices(country_name, bidding_zone, start_dt, end_dt):
    try:
        cet = pytz.timezone("Europe/Berlin")
        period_start = start_dt.astimezone(cet).strftime("%Y%m%d%H%M")
        period_end = end_dt.astimezone(cet).strftime("%Y%m%d%H%M")

        API_URL = "https://web-api.tp.entsoe.eu/api"
        PARAMS = {
            "securityToken": SECURITY_TOKEN,
            "documentType": "A44",
            "in_Domain": bidding_zone,
            "out_Domain": bidding_zone,
            "periodStart": period_start,
            "periodEnd": period_end,
            "contract_MarketAgreement.type": "A01"
        }

        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        ns = {'ns': root.tag.split('}')[0].strip('{')}
        data = []

        for ts in root.findall(".//ns:TimeSeries", ns):
            for period in ts.findall(".//ns:Period", ns):
                start_time_str = period.find("ns:timeInterval/ns:start", ns).text
                start_time_utc = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
                resolution = period.find("ns:resolution", ns).text
                for point in period.findall("ns:Point", ns):
                    position_el = point.find("ns:position", ns)
                    price_el = point.find("ns:price.amount", ns)
                    if position_el is None or price_el is None:
                        continue
                    position = int(position_el.text)
                    price = float(price_el.text)
                    mtu_start, mtu_end = get_time_interval(start_time_utc, resolution, position)
                    delivery_period = f"{mtu_start.strftime('%d.%m.%Y %H:%M')} - {mtu_end.strftime('%d.%m.%Y %H:%M')} (UTC)"
                    data.append({
                        "delivery_period": delivery_period,
                        "price_eur_mwh": price,
                        "resolution": resolution,
                        "bidding_zone": bidding_zone,
                        "country": country_name
                    })

        if not data:
            logging.warning(f"No Day-ahead data for {country_name} {period_start} - {period_end}")
            return

        df = pd.DataFrame(data)
        conn = psycopg2.connect(dbname=AZURE_PG_DB, user=AZURE_PG_USER, password=AZURE_PG_PASSWORD,
                                host=AZURE_PG_HOST, port=5432, sslmode='require')
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS day_ahead_prices (
            delivery_period TEXT,
            price_eur_mwh DOUBLE PRECISION,
            resolution TEXT,
            bidding_zone TEXT,
            country TEXT,
            inserted_at TIMESTAMP DEFAULT now()
        )
        """)
        conn.commit()
        records = df.to_dict("records")
        columns = df.columns.tolist()
        values = [[r.get(col) for col in columns] for r in records]
        execute_values(cursor,
                       f"INSERT INTO day_ahead_prices ({', '.join(columns)}) VALUES %s",
                       values)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Inserted {len(df)} Day-ahead rows for {country_name} {period_start} - {period_end}")
    except Exception as e:
        logging.error(f"Day-ahead ETL failed for {country_name} {period_start} - {period_end}: {e}", exc_info=True)
        send_email_alert("ENTSOE Day-ahead ETL Failed", f"{country_name} {period_start}-{period_end}\n{e}")
        raise

# --- Day-wise Historical Loader ---
def historical_load_daywise():
    start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    now_utc = datetime.now(timezone.utc)
    cet = pytz.timezone("Europe/Berlin")
    yesterday_22_cet = now_utc.astimezone(cet).replace(hour=22, minute=0, second=0, microsecond=0)
    if now_utc >= yesterday_22_cet.astimezone(timezone.utc):
        historical_end = yesterday_22_cet
    else:
        historical_end = yesterday_22_cet - timedelta(days=1)

    current_day = start_date
    while current_day < historical_end.astimezone(timezone.utc):
        next_day = current_day + timedelta(days=1)
        logging.info(f"Fetching historical data for {current_day.strftime('%Y-%m-%d')}")
        for tso_name, control_area in germany_control_areas.items():
            fetch_and_store_data(f"Germany-{tso_name}", control_area, current_day, next_day)
        fetch_and_store_dayahead_prices("BZN|DE-LU", germany_bidding_zone, current_day, next_day)
        current_day = next_day

# --- Daily Loader ---
def daily_load():
    now_utc = datetime.now(timezone.utc)
    cet = pytz.timezone("Europe/Berlin")
    today_22_cet = now_utc.astimezone(cet).replace(hour=22, minute=0, second=0, microsecond=0)
    if now_utc >= today_22_cet.astimezone(timezone.utc):
        period_end = today_22_cet
    else:
        period_end = today_22_cet - timedelta(days=1)
    period_start = period_end - timedelta(days=1)

    for tso_name, control_area in germany_control_areas.items():
        fetch_and_store_data(f"Germany-{tso_name}", control_area, period_start, period_end)
    fetch_and_store_dayahead_prices("BZN|DE-LU", germany_bidding_zone, period_start, period_end)

# --- Entry ---
if __name__ == "__main__":
    try:
        last_run_file = ".last_historical_run"
        if not os.path.exists(last_run_file):
            logging.info("Running historical backfill from Jan 1, 2024...")
            historical_load_daywise()
            with open(last_run_file, "w") as f:
                f.write(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

        logging.info("Running daily load...")
        daily_load()
        logging.info("ETL completed successfully.")
    except Exception as e:
        logging.error(f"ETL failed: {e}", exc_info=True)
        send_email_alert("ENTSOE ETL Failed", str(e))
