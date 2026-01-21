import subprocess
import sys

def run_script(script):
    print(f"\n▶ Running {script}")
    result = subprocess.run(
        ["python", f"scripts/{script}"],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        sys.exit(1)

def main():
    print("\n==============================")
    print(" STARTING DATA QUALITY PIPELINE ")
    print("==============================")

    run_script("generate_raw_source_data.py")
    run_script("clean_source_data.py")
    run_script("generate_target_data.py")
    run_script("validate_data_quality.py")

    launch = input("\nLaunch dashboard? (y/n): ").lower()
    if launch == "y":
        run_script("dq_dashboard.py")

    print("\n==============================")
    print(" PIPELINE COMPLETED SUCCESSFULLY ")
    print("==============================")

if __name__ == "__main__":
    main()
