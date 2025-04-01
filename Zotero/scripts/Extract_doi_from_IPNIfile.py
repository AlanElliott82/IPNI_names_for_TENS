#Part of the IPNI workflow
#Extract doi's from IPNI rows. *UPDATE THE month variable to corrospond to the IPNI table you are working in*

import sqlite3
import re
import csv

month = 'Feb'

# Connect to your SQLite database
conn = sqlite3.connect("data/WFOsqlite.db")
cursor = conn.cursor()

# Function to extract DOI using regex
def extract_doi(text):
    if text:
        # DOI regex pattern
        pattern = r'\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group()
    return None

# Query the database to get remarks (where doi is) and family. Update the IPNI(Month) table name.
cursor.execute(f"SELECT remarks_s_lower, family_s_lower FROM IPNI{month}")
rows = cursor.fetchall()

# Process rows, extracting DOIs and removing duplicates
doi_family_mapping = {}
for remarks, family in rows:
    doi = extract_doi(remarks)
    if doi:
        # Use DOI as key to ensure uniqueness
        doi_family_mapping[doi] = family

# Get the list of all tables in the database, excluding IPNI tables and Cichorieae which is a genus table.
query_tables = '''
SELECT name 
FROM sqlite_master 
WHERE type='table' AND name NOT LIKE '%IPNI%' AND name != 'Cichorieae'
'''

cursor.execute(query_tables)
tables = [row[0] for row in cursor.fetchall()]

# Process the extracted DOIs and query matching tables
output = []
for doi, family in doi_family_mapping.items():
    collection_found = False

    # Check the family against the families in each table
    for table in tables:
        query_check_family = f'''
        SELECT 1 
        FROM {table} 
        WHERE Family = ?
        '''
        cursor.execute(query_check_family, (family,))
        result = cursor.fetchone()
        
        if result:
            # If the matching table is 'TENFams', set Collection to the family name
            if table == 'TENFams':
                collection = family
            else:
                collection = table  # Use the table name as the collection if its a Order level TENs or things like legumes where IPNI uses variants of the accetped family.
            collection_found = True
            break  # Out when match is found
    
    if not collection_found: 
        collection = '2025 No Taxonomic Expert Network'  # Default value if no match is found
    
    # Append result
    output.append((doi, family, collection))

# Write results to a CSV file
output_csv = f"Zotero/data/Zotero_{month}.csv"
with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    # Write header
    writer.writerow(["DOI", "Family", "Collection"])
    # Write data
    for row in output:
        writer.writerow(row)

print(f"Results written to {output_csv}")

# Close the database connection
conn.close()