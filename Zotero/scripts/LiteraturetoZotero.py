import csv
import requests
import re
import sys

sys.path.append('Not_For_Repo/')

import ZoteroAccess

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
    cache_key = (collection_name, parent_collection)
    if cache_key in collection_cache:
        return collection_cache[cache_key]  # Return cached collection key if it exists

    url = f"https://api.zotero.org/{ZoteroAccess.ZOTERO_LIBRARY_TYPE}s/{ZoteroAccess.ZOTERO_USER_ID}/collections"
    headers = {"Zotero-API-Key": ZoteroAccess.ZOTERO_API_KEY}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        collections = response.json()
        for collection in collections:
            if collection['data']['name'] == collection_name and (not parent_collection or collection['data']['parentCollection'] == parent_collection):
                collection_cache[cache_key] = collection['data']['key']  # Cache the collection key
                return collection['data']['key']

        # Create collection if it doesn't exist
        payload = [{"name": collection_name, "parentCollection": parent_collection}]
        create_response = requests.post(url, json=payload, headers=headers)
        try:
            response_json = create_response.json()
            if 'successful' in response_json and '0' in response_json['successful']:
                new_key = response_json['successful']['0']['data']['key']
                collection_cache[cache_key] = new_key  # Cache the new collection key
                return new_key
            else:
                print(f"Unexpected response structure: {response_json}")
                return None
        except ValueError:
            print("Failed to parse JSON response from Zotero")
            return None
    else:
        print(f"Failed to fetch collections. Status code: {response.status_code}, Response: {response.text}")
        return None

# Function to add an item to a collection
def add_item_to_collection(metadata, collection_key):
    url = f"https://api.zotero.org/{ZoteroAccess.ZOTERO_LIBRARY_TYPE}s/{ZoteroAccess.ZOTERO_USER_ID}/items"
    headers = {"Zotero-API-Key": ZoteroAccess.ZOTERO_API_KEY, "Content-Type": "application/json"}
    metadata['collections'] = [collection_key]
    payload = [metadata]
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        print(f"Item added successfully. Response: {response.json()}")
    else:
        print(f"Error adding item. Status: {response.status_code}, Details: {response.text}")

# Ensure only one "Taxonomic Expert Networks" parent collection exists
if "Taxonomic Expert Networks" not in collection_cache:
    taxonomic_network_key = get_or_create_collection("Taxonomic Expert Networks")
    collection_cache["Taxonomic Expert Networks"] = taxonomic_network_key
else:
    taxonomic_network_key = collection_cache["Taxonomic Expert Networks"]

# Cache the "2025 No Taxonomic Expert Network" collection
if "2025 No Taxonomic Expert Network" not in collection_cache:
    collection_cache["2025 No Taxonomic Expert Network"] = get_or_create_collection("2025 No Taxonomic Expert Network")

# Open the CSV file and process rows
with open("Zotero/data/Zotero_Jan.csv", mode="r", encoding="utf-8-sig") as file:
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
            if collection_name == "2025 No Taxonomic Expert Network":
                subcollection_key = collection_cache["2025 No Taxonomic Expert Network"]  # Use cached key
            else:
                # Create subcollection under "Taxonomic Expert Networks"
                subcollection_key = get_or_create_collection("inbox", parent_collection=get_or_create_collection(collection_name, parent_collection=taxonomic_network_key))

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