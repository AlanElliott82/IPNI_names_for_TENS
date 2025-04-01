import json

# Input and output file paths
input_file = "zotero_collection_cache.json"
output_file = "zotero_collection_cache_fixed.json"

# Load the JSON data
with open(input_file, "r", encoding="utf-8") as file:
    data = json.load(file)

# Convert list to dictionary format, using 'data' field and filtering out deleted items
collection_dict = {
    entry["data"]["name"]: entry["data"]["key"]
    for entry in data
    if isinstance(entry, dict) and "data" in entry and "name" in entry["data"] and "key" in entry["data"] and not entry["data"].get("deleted", False)
}

# Save the transformed data
with open(output_file, "w", encoding="utf-8") as file:
    json.dump(collection_dict, file, indent=4)

print(f"Converted JSON saved to {output_file}")