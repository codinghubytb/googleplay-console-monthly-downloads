from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import io
import re


class PlayStoreStats:
    def __init__(self, service_account_file, bucket_name, package_name):
        self.package_name = package_name
        self.bucket_name = bucket_name
        self.client = self._authenticate(service_account_file)
    
    def _authenticate(self, service_account_file):
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file
        )
        return storage.Client(credentials=credentials)
    
    def list_overview_reports(self):
        bucket = self.client.bucket(self.bucket_name)
        prefix = f"stats/installs/installs_{self.package_name}_"
        blobs = bucket.list_blobs(prefix=prefix)
        
        reports = []
        for blob in blobs:
            if "_overview.csv" in blob.name:
                match = re.search(r"_(\d{6})_overview\.csv", blob.name)
                if match:
                    report_date = match.group(1)
                    reports.append({
                        "name": blob.name,
                        "year": int(report_date[:4]),
                        "month": int(report_date[4:]),
                        "period": f"{report_date[:4]}-{report_date[4:]:0>2}",
                    })
        
        return sorted(reports, key=lambda x: (x["year"], x["month"]))
    
    def download_report(self, blob_name):
        try:
            bucket = self.client.bucket(self.bucket_name)
            content = bucket.blob(blob_name).download_as_bytes()
            
            for encoding in ["utf-16", "utf-8", "latin-1"]:
                try:
                    return pd.read_csv(io.BytesIO(content), encoding=encoding)
                except Exception:
                    continue
            
            return None
        except Exception:
            return None
    
    def get_all_downloads(self):
        reports = self.list_overview_reports()
        if not reports:
            return pd.DataFrame()
        
        all_data = []
        for report in reports:
            df = self.download_report(report["name"])
            if df is not None:
                df["report_period"] = report["period"]
                all_data.append(df)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        
        return pd.DataFrame()


def analyze_downloads(df: pd.DataFrame):
    if df.empty:
        return []

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date")

    month_names = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    results = []
    for period in sorted(df["report_period"].unique()):
        month_df = df[df["report_period"] == period]
        if month_df.empty:
            continue
        
        max_installs = int(month_df["Active Device Installs"].max())

        year, month = period.split("-")
        month_short = month_names[int(month)]

        results.append({
            "month": f"{month_short} {year}",
            "activeUsers": max_installs
        })
    
    return results


def get_playstore_monthly_installs(service_account_file, bucket_name, package_name):
    stats = PlayStoreStats(service_account_file, bucket_name, package_name)
    df = stats.get_all_downloads()
    return analyze_downloads(df)
