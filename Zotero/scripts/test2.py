import csv
import json
import os
import requests
import sys

sys.path.append('Not_For_Repo/')
import ZoteroAccess

# Path to cache file
CACHE_FILE_PATH = "Zotero/scripts/zotero_collection_cache_fixed.json"

# Load Zotero collections cache from a JSON file
def load_collections_cache(file_path):
    """Load the collection cache from a JSON file."""
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            try:
                return json.load(file)
            except json.JSONDecodeError:
                print("⚠️ Error: Invalid JSON format in cache file.")
                return {}
    else:
        print("⚠️ Cache file not found. Using an empty cache.")
        return {}

# Save the updated cache back to file
def save_collections_cache(cache, file_path):
    """Save the updated collection cache to a JSON file."""
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(cache, file, indent=4)

# Initialize the cache
ZOTERO_COLLECTIONS_CACHE = load_collections_cache(CACHE_FILE_PATH)

def get_or_create_collection(collection_name, parent_key=None):
    """Fetch or create a Zotero collection."""
    def find_collection_in_cache(parent, name):
        """Search for an existing collection in the parent's children."""
        if parent:
            for child in parent.get("children", []):
                if child["name"] == name:
                    return child
        return None

    # Locate the parent collection in the cache
    parent = ZOTERO_COLLECTIONS_CACHE.get(parent_key) if parent_key else None

    # Check if the collection already exists
    collection = find_collection_in_cache(parent, collection_name)
    if collection:
        return collection["key"]

    # Create the collection via Zotero API
    headers = {"Zotero-API-Key": ZoteroAccess.ZOTERO_API_KEY}
    payload = [{"name": collection_name, "parentCollection": parent_key}]
    response = requests.post(f"https://api.zotero.org/{ZoteroAccess.ZOTERO_LIBRARY_TYPE}s/{ZoteroAccess.ZOTERO_USER_ID}/collections", headers=headers, json=payload)

    if response.status_code in [200, 201]:
        # Successfully created the collection, add to cache
        response_json = response.json()
        new_key = response_json["success"]["0"]
        print(f"🆕 Created new collection: {collection_name} (Key: {new_key})")

        # Prepare the new collection structure
        new_collection = {"name": collection_name, "key": new_key, "children": []}
        if parent:
            # Append to parent's children to maintain hierarchy
            if not find_collection_in_cache(parent, collection_name):  # Prevent duplicates
                parent["children"].append(new_collection)
        else:
            # Add to top-level cache if no parent
            if collection_name not in ZOTERO_COLLECTIONS_CACHE:  # Prevent top-level duplicates
                ZOTERO_COLLECTIONS_CACHE[collection_name] = new_collection

        # Save the updated cache
        save_collections_cache(ZOTERO_COLLECTIONS_CACHE, CACHE_FILE_PATH)
        return new_key
    else:
        print(f"⚠️ Error creating collection '{collection_name}': {response.status_code}, {response.text}")
        return None

def add_doi_to_zotero(doi, collection_key):
    """Add DOI metadata to a Zotero collection."""
    headers = {"Zotero-API-Key": ZoteroAccess.ZOTERO_API_KEY, "Content-Type": "application/json"}

    # Retrieve metadata from CrossRef API
    crossref_url = f"https://api.crossref.org/works/{doi}"
    response = requests.get(crossref_url)

    if response.status_code != 200:
        print(f"⚠️ Error fetching metadata for DOI {doi}: {response.status_code}")
        return

    metadata = response.json().get("message", {})
    item = {
        "itemType": "journalArticle",
        "title": metadata.get("title", ["Unknown Title"])[0],
        "creators": [{"creatorType": "author", "name": "Unknown Author"}],
        "abstractNote": metadata.get("abstract", ""),
        "DOI": doi,
        "url": metadata.get("URL", ""),
        "date": "-".join(map(str, metadata.get("issued", {}).get("date-parts", [["Unknown"]])[0])),
        "collections": [collection_key]
    }

    # Add the metadata to Zotero
    zotero_url = f"https://api.zotero.org/{ZoteroAccess.ZOTERO_LIBRARY_TYPE}s/{ZoteroAccess.ZOTERO_USER_ID}/items"
    response = requests.post(zotero_url, headers=headers, json=[item])

    if response.status_code == 200:
        print(f"✅ Successfully added DOI {doi} to collection (Key: {collection_key})")
    else:
        print(f"⚠️ Error adding DOI {doi}: {response.status_code}, {response.text}")

def process_csv(file_path):
    """Process a CSV to create hierarchical collections and import DOIs."""
    with open(file_path, mode="r", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            doi = row.get("DOI", "").strip()
            family = row.get("Family", "").strip()
            collection_name = row.get("Collection", "").strip()

            if not doi or not family or not collection_name:
                print(f"⚠️ Skipping row due to missing DOI, Family, or Collection: {row}")
                continue

            if collection_name == "2025 No Taxonomic Expert Network":
                # Add DOI directly to "2025 No Taxonomic Expert Network" collection
                collection_key = get_or_create_collection("2025 No Taxonomic Expert Network", None)
                add_doi_to_zotero(doi, collection_key)
            else:
                # Create the family subcollection under "Taxonomic Expert Networks"
                parent_key = get_or_create_collection("Taxonomic Expert Networks", None)
                family_key = get_or_create_collection(collection_name, parent_key)

                # Create an "inbox" subcollection under the family
                inbox_key = get_or_create_collection("inbox", family_key)
                add_doi_to_zotero(doi, inbox_key)

# Process the CSV file
process_csv("Zotero/data/Zotero_Jan.csv")