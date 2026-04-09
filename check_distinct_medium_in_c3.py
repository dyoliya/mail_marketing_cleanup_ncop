import os
import pandas as pd

CACHE_FOLDER = "local_c3_cache"

def get_distinct_opt_out_medium(folder=CACHE_FOLDER):
    distinct_values = set()

    files = [
        f for f in os.listdir(folder)
        if f.endswith((".csv", ".xlsx", ".xls"))
    ]

    total_files = len(files)
    print(f"Found {total_files} supported files.\n")

    for i, file in enumerate(files, start=1):
        file_path = os.path.join(folder, file)
        print(f"[{i}/{total_files}] Processing: {file}")

        if file.endswith(".csv"):
            try:
                chunk_count = 0
                chunks = pd.read_csv(file_path, usecols=["OPT-OUT_MEDIUM"], chunksize=10000)

                for chunk_count, chunk in enumerate(chunks, start=1):
                    vals = chunk["OPT-OUT_MEDIUM"].dropna().unique()
                    distinct_values.update(vals)

                    if chunk_count % 10 == 0:
                        print(f"    processed {chunk_count} chunks...")

                print(f"    done ({chunk_count} chunks)")

            except Exception as e:
                print(f"    Skipping {file}: {e}")

        elif file.endswith((".xlsx", ".xls")):
            try:
                sheets = pd.read_excel(file_path, sheet_name=None, usecols=["OPT-OUT_MEDIUM"])
                sheet_names = list(sheets.keys())
                total_sheets = len(sheet_names)

                for s_idx, (sheet_name, df) in enumerate(sheets.items(), start=1):
                    vals = df["OPT-OUT_MEDIUM"].dropna().unique()
                    distinct_values.update(vals)
                    print(f"    sheet {s_idx}/{total_sheets}: {sheet_name}")

                print(f"    done ({total_sheets} sheets)")

            except Exception as e:
                print(f"    Skipping {file}: {e}")

    return distinct_values


def main():
    values = get_distinct_opt_out_medium()

    print("\nDistinct OPT-OUT_MEDIUM values:\n")
    for val in sorted(values):
        print(val)


if __name__ == "__main__":
    main()