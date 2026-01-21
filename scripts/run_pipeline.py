import subprocess
import sys

def run_step(description, command):
    print(f"\n--- {description} ---")
    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        print(f"\n❌ Failed at step: {description}")
        sys.exit(1)

    print(f"✅ Completed: {description}")


def main():
    print("\n==============================")
    print(" STARTING DATA QUALITY PIPELINE ")
    print("==============================")

    # 1. Generate raw source data
    run_step(
        "Generating raw source data",
        "python scripts/generate_raw_source_data.py"
    )

    # 2. Clean source data
    run_step(
        "Cleaning source data",
        "python scripts/clean_source_data.py"
    )

    # 3. Generate faulty target data
    run_step(
        "Generating target data with defects",
        "python scripts/generate_target_data.py"
    )

    # 4. Run data quality validations
    run_step(
        "Running data quality validations",
        "python scripts/validate_data_quality.py"
    )

    print("\n==============================")
    print(" PIPELINE EXECUTION COMPLETED ")
    print("==============================")

    # Ask user if dashboard is needed
    choice = input("\nDo you want to launch the dashboard? (y/n): ").strip().lower()

    if choice == "y":
        print("\nLaunching dashboard...")
        subprocess.run("streamlit run scripts/dq_dashboard.py", shell=True)
    else:
        print("Dashboard skipped.")


if __name__ == "__main__":
    main()
