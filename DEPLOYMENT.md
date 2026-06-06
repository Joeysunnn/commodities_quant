# Deployment And Daily Updates

This project is easiest to revive as:

1. A managed PostgreSQL database that stores `clean.observations` and `clean.load_runs`.
2. GitHub Actions that writes fresh daily data into that database.
3. A hosted Streamlit app that reads the same database and gives you a shareable URL.

## Required Secrets

Use one of these two configuration styles everywhere.

Preferred:

```env
DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DB_NAME?sslmode=require
```

Compatible with the existing scripts:

```env
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=5432
DB_NAME=commodities_db
DB_SSLMODE=require
```

`DATABASE_URL` wins when both are set.

## GitHub Actions Daily Update

Add the database configuration above to GitHub repository secrets.

Then open:

```text
Actions -> Daily Quant Data Update -> Run workflow
```

The workflow also runs automatically every day at 09:00 UTC. It uploads logs as an artifact named `daily-update-logs`, which is the first place to check if a data source breaks.

The workflow sets `ALLOW_PARTIAL_DAILY_UPDATE=1`. This keeps the scheduled job green when one external source blocks scraping, while the log still records the failed source count.

## Hosted Streamlit App

Deploy `app.py` as the Streamlit entry point and set the same database secret in the hosting platform.

Recommended minimal setup:

- Main file: `app.py`
- Python: `runtime.txt`
- Dependencies: `requirements.txt`
- Streamlit config: `.streamlit/config.toml`
- Secrets: `DATABASE_URL` or the five `DB_*` values above

After deployment, the hosting platform will give you a public HTTPS URL. Open that URL from another device to confirm the dashboard loads from the remote PostgreSQL database, not from a local machine.

## First Revival Checklist

1. Create or choose the remote PostgreSQL database.
2. Create schema/table objects if they do not already exist.
3. Import the historical data once with `python database/main.py --all`, or migrate from the old local database.
4. Add database secrets to GitHub Actions.
5. Manually run `Daily Quant Data Update` once.
6. Deploy the Streamlit app and add the same database secrets.
7. Open the hosted URL on another device.

## Notes

- The LME updater uses Selenium and runs headless in GitHub Actions.
- The current CME COMEX delivery report URLs can return `403` with an IP-block message. Treat COMEX as an upstream access issue when logs show that response.
- External data sources can still fail because of website or network changes.
- If the app loads but shows no data, verify that `clean.observations` in the remote database has recent rows.
