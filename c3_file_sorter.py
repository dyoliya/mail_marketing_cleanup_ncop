import os
import shutil

CACHE_FOLDER = "local_c3_cache"

PHONE_FOLDER = os.path.join(CACHE_FOLDER, "Phone")
ADDRESS_FOLDER = os.path.join(CACHE_FOLDER, "Address")
EMAIL_FOLDER = os.path.join(CACHE_FOLDER, "Email")


def create_folders():
    os.makedirs(PHONE_FOLDER, exist_ok=True)
    os.makedirs(ADDRESS_FOLDER, exist_ok=True)
    os.makedirs(EMAIL_FOLDER, exist_ok=True)


def sort_files():
    files = os.listdir(CACHE_FOLDER)

    for file in files:
        file_path = os.path.join(CACHE_FOLDER, file)

        # skip directories
        if os.path.isdir(file_path):
            continue

        lower_name = file.lower()

        if "phone" in lower_name:
            destination = os.path.join(PHONE_FOLDER, file)

        elif "address" in lower_name:
            destination = os.path.join(ADDRESS_FOLDER, file)

        elif "email" in lower_name:
            destination = os.path.join(EMAIL_FOLDER, file)

        else:
            print(f"Skipping (no category): {file}")
            continue

        print(f"Moving {file} -> {os.path.basename(os.path.dirname(destination))}/")
        shutil.move(file_path, destination)


def main():
    create_folders()
    sort_files()
    print("\n✅ Sorting complete.")


if __name__ == "__main__":
    main()