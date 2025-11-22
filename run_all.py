import subprocess
import sys
import time

scripts = ["scraper_1.py"]

for script in scripts:
    print(f"\n=== Running {script} ===")
    start = time.time()
    result = subprocess.call([sys.executable, script])
    end = time.time()

    print(f"{script} finished in {end - start:.2f} seconds")

    if result != 0:
        print(f"ERROR: {script} failed with exit code {result}")
        break
