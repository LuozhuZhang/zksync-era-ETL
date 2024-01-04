import os
import csv

def json_to_csv(json_data, csv_file_path, default_keys=None):
  # Collect all possible field names
  all_fieldnames = set()
  for entry in json_data:
    all_fieldnames.update(entry.keys())

  if not json_data:
    if default_keys is None:
      print(f"  ⚠️ Empty JSON data, skipping writing to {os.path.basename(csv_file_path)}")
      return
    else:
      keys = default_keys
      print(f"  ⚠️ Empty JSON data, creating empty CSV file {os.path.basename(csv_file_path)}")
  else:
    keys = default_keys if default_keys is not None else list(all_fieldnames)

  with open(csv_file_path, 'w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=keys)
    writer.writeheader()
    if json_data:
      writer.writerows(json_data)
