import csv
import requests
import json
import os
import sys

sys.path.append('Not_For_Repo/')

import ZoteroAccess

# Load Zotero collections cache from a JSON file
def load_collections_cache(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            try:
                return json.load(file)
            except json.JSONDecodeError:
                print("⚠️ Error: Invalid JSON format in collections cache file.")
                return {}
    else:
        print("⚠️ Collections cache file not found. Using an empty cache.")
        return {}

# Path to the JSON file
CACHE_FILE_PATH = "Zotero/scripts/zotero_collection_cache_fixed.json"

# Load ZOTERO_COLLECTIONS_CACHE from the file
ZOTERO_COLLECTIONS_CACHE = load_collections_cache(CACHE_FILE_PATH)

# Function to get or create a Zotero collection
def get_or_create_collection(collection_name, parent_collection_key=None):
    """Fetch or create a Zotero collection."""
    # Determine the cache key for collections
    cache_key = f"{parent_collection_key}_{collection_name}" if parent_collection_key else collection_name

    # Helper function to find a collection within the parent's children
def get_or_create_collection(collection_name, parent_collection_key=None):
    """Fetch or create a Zotero collection."""
    # Helper function to find a collection within parent's children
    def find_collection_in_children(parent_key, name):
        if parent_key:
            parent = ZOTERO_COLLECTIONS_CACHE.get(parent_key)
            if parent and "children" in parent:
                for child in parent["children"]:
                    if child["name"] == name:
                        return child
        return None

    # Find or create collection logic
    parent = ZOTERO_COLLECTIONS_CACHE.get(parent_collection_key) if parent_collection_key else None
    collection = find_collection_in_children(parent_collection_key, collection_name)
    
    if collection:
        # If collection exists, return its key
        return collection["key"]

    # Create the collection via Zotero API
    headers = {"Zotero-API-Key": ZoteroAccess.ZOTERO_API_KEY}
    base_url = f"https://api.zotero.org/{ZoteroAccess.ZOTERO_LIBRARY_TYPE}s/{ZoteroAccess.ZOTERO_USER_ID}/collections"
    payload = [{"name": collection_name, "parentCollection": parent_collection_key}]
    response = requests.post(base_url, headers=headers, json=payload)

    if response.status_code in [200, 201]:
        response_json = response.json()
        if "success" in response_json and "0" in response_json["success"]:
            new_key = response_json["success"]["0"]
            print(f"🆕 Created new collection: {collection_name} (Key: {new_key})")

            # Create structure for cache
            new_collection = {"name": collection_name, "key": new_key, "children": []}

            # Append to parent or add to top level
            if parent:
                if not find_collection_in_children(parent_collection_key, collection_name):
                    parent["children"].append(new_collection)
            else:
                ZOTERO_COLLECTIONS_CACHE[collection_name] = new_collection

            # Save updated cache
            with open(CACHE_FILE_PATH, "w", encoding="utf-8") as file:
                json.dump(ZOTERO_COLLECTIONS_CACHE, file, indent=4)

            return new_key
    else:
        print(f"Error creating collection '{collection_name}': {response.status_code}, {response.text}")
        return None

# Function to add DOI metadata to Zotero
def add_doi_to_zotero(doi, collection_key):
    headers = {"Zotero-API-Key": ZoteroAccess.ZOTERO_API_KEY, "Content-Type": "application/json"}

    # Retrieve metadata from CrossRef
    crossref_url = f"https://api.crossref.org/works/{doi}"
    response = requests.get(crossref_url)
    if response.status_code != 200:
        print(f"Error fetching metadata for DOI {doi}: {response.status_code}, {response.text}")
        return

    metadata = response.json()["message"]
    item = {
        "itemType": "journalArticle",
        "title": metadata.get("title", ["Unknown Title"])[0],
        "creators": [{"creatorType": "author", "name": "Unknown Author"}],
        "abstractNote": metadata.get("abstract", ""),
        "DOI": doi,
        "url": metadata.get("URL", ""),
        "date": "-".join(map(str, metadata.get("published-print", {}).get("date-parts", [["Unknown"]])[0])),
        "collections": [collection_key]
    }

    # Add the item to Zotero
    zotero_url = f"https://api.zotero.org/{ZoteroAccess.ZOTERO_LIBRARY_TYPE}s/{ZoteroAccess.ZOTERO_USER_ID}/items"
    response = requests.post(zotero_url, headers=headers, json=[item])
    if response.status_code == 200:
        print(f"✅ Successfully added DOI {doi} to Zotero in collection {collection_key}.")
    else:
        print(f"Error adding DOI {doi} to Zotero: {response.status_code}, {response.text}")

# Main script to process the CSV and add DOIs to Zotero
def process_csv(file_path):
    with open(file_path, mode="r", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            doi = row.get("DOI", "").strip()
            collection_name = row.get("Collection", "").strip()

            if not doi or not collection_name:
                print(f"Skipping row due to missing DOI or Collection: {row}")
                continue

            if collection_name == "2025 No Taxonomic Expert Network":
                parent_collection_key = get_or_create_collection("2025 No Taxonomic Expert Network", None)
                target_collection_key = parent_collection_key
            else:
                taxonomic_network_key = get_or_create_collection("Taxonomic Expert Networks", None)
                parent_collection_key = get_or_create_collection(collection_name, taxonomic_network_key)

                # Ensure inbox is nested under parent collection
                inbox_key = get_or_create_collection("inbox", parent_collection_key)
                target_collection_key = inbox_key



            if target_collection_key:
                add_doi_to_zotero(doi, target_collection_key)

process_csv("Zotero/data/Zotero_Jan.csv")