# Google Play Console Monthly Downloads
A lightweight Python module to retrieve monthly install statistics from the Google Play Console export bucket (Google Cloud Storage).
The script:
- Connects using a service account
- Lists Play Store install overview reports
- Downloads and parses monthly CSV files
- Returns clean aggregated monthly stats
- No printing, no logging, fully API-friendly

## Installation

```
pip install -r requirements.txt
```

## Requirements

Before using this module, make sure:

1. Play Console data export to Google Cloud Storage is enabled
2. Your service account has Storage Object Viewer access
3. You downloaded the serviceaccount.json file
4. Your bucket / Uri Cloud Storage:
   ```
   pubsite_prod_xxxxxxxxxxxxxxxxxxx
   ```
   
## Usage Example

```python
from src.playstore_stats import get_playstore_monthly_installs

SERVICE_ACCOUNT_FILE = \"serviceaccount.json\"
BUCKET = \"pubsite_prod_xxxxxxxxxxxxxxxxxxx\"
PACKAGE = \"com.example.app\"

stats = get_playstore_monthly_installs(
    SERVICE_ACCOUNT_FILE,
    BUCKET,
    PACKAGE
)

print(stats)
```

## Example Output

```json
[
  { \"month\": \"Sep 2024\", \"activeUsers\": 220 },
  { \"month\": \"Oct 2024\", \"activeUsers\": 250 }
]
```

## Project Structure

```
playstore-stats/
│
├── src/
│   └── playstore_stats.py
│
├── example.py
├── requirements.txt
├── README.md
└── .gitignore
```

## Notes

- Returns an empty list when no data is found
- Supports CSVs encoded in UTF-16, UTF-8, Latin-1
- Ideal for integration with cron jobs, FastAPI, Django, backend services, etc.
