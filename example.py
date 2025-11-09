from src.playstore_stats import get_playstore_monthly_installs

# Fill with your real paths
SERVICE_ACCOUNT_FILE = "serviceaccount.json"
BUCKET = "your-play-console-bucket"
PACKAGE = "com.example.app"

if __name__ == "__main__":
    stats = get_playstore_monthly_installs(
        SERVICE_ACCOUNT_FILE,
        BUCKET,
        PACKAGE
    )
    print(stats)
