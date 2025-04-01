import csv
import requests
import re
#import sys

#sys.path.append('Not_For_Repo/')

#import ZoteroAccess

ZOTERO_API_KEY = "V6wR7rRqgvPGyJKf0YdTy1L6"
ZOTERO_USER_ID = "15714349"
ZOTERO_LIBRARY_TYPE = "user"  # Or 'user' for personal libraries

# Function to remove HTML tags using regex
def strip_html(html):
    if not html:  # Handle cases where 'html' might be None
        return ""
    clean = re.sub(r'<.*?>', '', html)
    return clean

# Cache for existing collections to prevent duplicates
collection_cache = {}

# Function to transform creators to match Zotero API requirements
def transform_creators(raw_creators):
    creators = []
    for creator in raw_creators:
        first_name = creator.get("given", "").strip()
        last_name = creator.get("family", "").strip()
        if first_name or last_name:
            creators.append({
                "creatorType": "author",
                "firstName": first_name,
                "lastName": last_name
            })
        else:
            print(f"Skipping invalid creator: {creator}")
    return creators

# Function to fetch or create a collection
def get_or_create_collection(collection_name, parent_collection=None):
    """Fetches an existing collection or creates a new one in Zotero."""
    cache_key = (collection_name, parent_collection)
    if cache_key in collection_cache:
        return collection_cache[cache_key]  # Return cached collection key if it exists

    headers = {"Zotero-API-Key": ZOTERO_API_KEY}
    base_url = f"https://api.zotero.org/{ZOTERO_LIBRARY_TYPE}s/{ZOTERO_USER_ID}/collections"
    
    # Step 1: Fetch **all** existing collections (handling pagination)
    collections = []
    start = 0
    while True:
        response = requests.get(f"{base_url}?limit=100&start={start}", headers=headers)
        if response.status_code != 200:
            print(f"Error fetching collections. Status: {response.status_code}, Response: {response.text}")
            return None
        batch = response.json()
        if not batch:  
            break  # No more collections to fetch
        collections.extend(batch)
        start += len(batch)  # Move to next batch of collections

    # Step 2: Check if the collection **already exists** under the correct parent
    for collection in collections:
        existing_name = collection['data']['name']
        existing_parent = collection['data'].get('parentCollection', None)

        if existing_name == collection_name and existing_parent == parent_collection:
            collection_key = collection['data']['key']
            print(f"✅ Collection already exists: {collection_name} (Key: {collection_key})")
            collection_cache[cache_key] = collection_key  # Cache it
            return collection_key

    # Step 3: Create collection if it does not exist
    payload = [{"name": collection_name, "parentCollection": parent_collection if parent_collection else None}]
    print(f"🆕 Creating new collection: {collection_name}, Parent: {parent_collection}")

    create_response = requests.post(base_url, json=payload, headers=headers)
    if create_response.status_code == 200 or create_response.status_code == 201:
        created_data = create_response.json()
        if "success" in created_data and "0" in created_data["success"]:
            new_key = created_data["success"]["0"]
            print(f"🎉 Successfully created collection: {collection_name} (Key: {new_key})")
            collection_cache[cache_key] = new_key  # Cache it
            return new_key
        else:
            print(f"⚠️ Unexpected response format when creating collection: {create_response.text}")
            return None
    else:
        print(f"❌ Failed to create collection. Status: {create_response.status_code}, Response: {create_response.text}")
        return None

# Function to add an item to a collection
def add_item_to_collection(metadata, collection_key):
    url = f"https://api.zotero.org/{ZOTERO_LIBRARY_TYPE}s/{ZOTERO_USER_ID}/items"
    headers = {"Zotero-API-Key": ZOTERO_API_KEY, "Content-Type": "application/json"}
    metadata['collections'] = [collection_key]
    payload = [metadata]
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        print(f"Item added successfully. Response: {response.json()}")
    else:
        print(f"Error adding item. Status: {response.status_code}, Details: {response.text}")

# Ensure only one "Taxonomic Expert Networks" parent collection exists
#if "Taxonomic Expert Networks" not in collection_cache:
 #   taxonomic_network_key = get_or_create_collection("Taxonomic Expert Networks")
  #  collection_cache["Taxonomic Expert Networks"] = taxonomic_network_key
#else:
 #   taxonomic_network_key = collection_cache["Taxonomic Expert Networks"]

# Ensure only one "Taxonomic Expert Networks" parent collection exists
print("Ensuring 'Taxonomic Expert Networks' parent collection exists...")
taxonomic_network_key = get_or_create_collection("Taxonomic Expert Networks")
collection_cache["Taxonomic Expert Networks"] = taxonomic_network_key
print(f"'Taxonomic Expert Networks' Key: {taxonomic_network_key}")

# Cache the "2025 No Taxonomic Expert Network" collection
if "2025 No Taxonomic Expert Network" not in collection_cache:
    collection_cache["2025 No Taxonomic Expert Network"] = get_or_create_collection("2025 No Taxonomic Expert Network")

# Open the CSV file and process rows
with open("Zotero/data/Zotero_Feb.csv", mode="r", encoding="utf-8-sig") as file:
    reader = csv.DictReader(file)
    reader.fieldnames = [header.strip().lower() for header in reader.fieldnames]
    for row in reader:
        doi = row.get('doi', "").strip()
        collection_name = row.get('collection', "").strip()

        if not doi or not collection_name:
            print(f"Skipping row due to missing DOI or collection name: {row}")
            continue

        response = requests.get(f"https://api.crossref.org/works/{doi}")
        if response.status_code == 200:
            metadata = response.json()["message"]
            if "published-print" in metadata:
                date = "-".join(map(str, metadata["published-print"]["date-parts"][0]))
            elif "published-online" in metadata:
                date = "-".join(map(str, metadata["published-online"]["date-parts"][0]))
            else:
                date = "Unknown"  # Default fallback value

            volume = metadata.get("volume", "").strip()
            issue = metadata.get("issue", "").strip()
            pages = metadata.get("page", "").strip()
            raw_abstract = metadata.get("abstract", "")
            abstract_note = strip_html(raw_abstract)
            doi = metadata.get("DOI", "").strip()
            url = metadata.get("URL", "").strip()
            journal_title = metadata.get("container-title", [""])[0].strip()

            # Remove HTML from the title
            raw_title = metadata.get("title", [""])[0]  # Fetch the title
            clean_title = strip_html(raw_title)  # Remove HTML tags

            # Subcollection placement
            # Ensure collection_name is under "Taxonomic Expert Networks"
            if collection_name == "2025 No Taxonomic Expert Network":
                subcollection_key = collection_cache["2025 No Taxonomic Expert Network"]  # Use cached key
            else:
                # Ensure the collection exists under "Taxonomic Expert Networks"
                parent_key = taxonomic_network_key  # Always use this as the parent
                subcollection_key = get_or_create_collection(collection_name, parent_collection=parent_key)

            if subcollection_key:
                zotero_metadata = {
                    "itemType": "journalArticle",
                    "title": clean_title,  # Use the cleaned title here
                    "creators": transform_creators(metadata.get("author", [])) or [{"creatorType": "author", "name": "Unknown Author"}],
                    "date": date,
                    "journalAbbreviation": journal_title,
                    "abstractNote": abstract_note,
                    "DOI": doi,
                    "url": url,
                    "collections": [subcollection_key],
                }
                if volume:
                    zotero_metadata["volume"] = volume
                if issue:
                    zotero_metadata["issue"] = issue
                if pages:
                    zotero_metadata["pages"] = pages
                add_item_to_collection(zotero_metadata, subcollection_key)

        else:
            print(f"Failed to fetch metadata for DOI: {doi}. Status code: {response.status_code}")