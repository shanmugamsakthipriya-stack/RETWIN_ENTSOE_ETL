import os

# ENTSOE
SECURITY_TOKEN = os.environ.get("SECURITY_TOKEN")

# Database
AZURE_PG_HOST = os.environ.get("AZURE_PG_HOST")
AZURE_PG_DB = os.environ.get("AZURE_PG_DB")
AZURE_PG_USER = os.environ.get("AZURE_PG_USER")
AZURE_PG_PASSWORD = os.environ.get("AZURE_PG_PASSWORD")

# Email
SMTP_SERVER = os.environ.get("SMTP_SERVER")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
ALERT_EMAIL = os.environ.get("ALERT_EMAIL")
