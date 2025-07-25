import pandas as pd
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

ABS_PATH = os.getcwd()
DEST_ROOT = os.path.join(ABS_PATH, "model", "data", "foodi-ml-dataset", "data")
S3_PREFIX = "s3://glovo-products-dataset-d1c9720d"

df = pd.read_csv(
    os.path.join(
        ABS_PATH,
        "model",
        "data",
        "foodi-ml-dataset",
        "data",
        "glovo-foodi-ml-dataset_filtered.csv",
    )
)
paths = df["s3_path"].tolist()
# Ensure the destination root exists once
os.makedirs(DEST_ROOT, exist_ok=True)


def download(path):
    local_path = os.path.join(DEST_ROOT, path)
    # Skip if file already exists
    if os.path.exists(local_path):
        return

    cmd = ["aws", "s3", "cp", f"{S3_PREFIX}/{path}", local_path, "--no-sign-request"]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


try:
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(download, path) for path in paths]

except KeyboardInterrupt:
    print("\n🛑 Interrupted by user. Cancelling remaining tasks...")
    for f in futures:
        f.cancel()

print(r"\n✅ Done. Total new files downloaded")
