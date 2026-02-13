import sqlite3
import csv
import datetime
import os
import shutil
from shutil import copy2

# Get the current time
time = datetime.datetime.now

# Connect to the SQLite database
conn = sqlite3.connect('data/WFOsqlite.db')
cursor = conn.cursor()

# Get table names
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
table_names = cursor.fetchall()

exclude_substrings = ['IPNI', 'EuroPlusMed']
table_names_array = [table[0] for table in table_names if not any(sub in table[0] for sub in exclude_substrings)]

month = "Jan26"  # Change each month

# ---------------------------------------------------------
# NEW SECTION: EuroPlusMed Geography Matching
# ---------------------------------------------------------

geo_query = f"""
SELECT DISTINCT 
    rhakhis_wfo AS WFOID,
    id AS IPNID,
    taxon_scientific_name_s_lower AS scientificName,
    authors_t AS authorship,
    reference_t AS namePublishedin,
    name_status_s_lower AS nomenclaturalStatus,
    basionym_s_lower AS originalName,
    basionym_author_s_lower AS originalNameAuthor,
    distribution_s_lower AS distribution
FROM (
    SELECT i.*
    FROM IPNI{month} AS i
    JOIN EuroPlusMed_Geography AS g
      ON i.distribution_s_lower LIKE '%' || LOWER(g.Country) || '%'

    UNION

    SELECT i.*
    FROM IPNI{month} AS i
    JOIN EuroPlusMed_TDWG AS g
      ON i.distribution_s_lower LIKE '%' || LOWER(g.Region) || '%'
);
"""

cursor.execute(geo_query)
geo_results = cursor.fetchall()
geo_count = len(geo_results)

# Create directory
geo_dir = f'IPNInewRecords/2026/EuroPlusMed'
os.makedirs(geo_dir, exist_ok=True)

geo_file = f'{geo_dir}/EuroPlusMed_{month}.csv'

if geo_count > 0:
    column_names = [description[0] for description in cursor.description]

    with open(geo_file, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(column_names)
        writer.writerows(geo_results)

    with open(f'IPNInewRecords/2026/Result_log_{month}.txt', 'a') as log_file:
        log_file.write(f'EuroPlusMed match executed at: {str(time())} - Count: {geo_count}\n')

else:
    with open(f'IPNInewRecords/2026/noResult_log_{month}.txt', 'a') as log_file:
        log_file.write(f'EuroPlusMed match executed at: {str(time())} - Count: 0\n')

# ---------------------------------------------------------
# EXISTING TENFam PROCESSING
# ---------------------------------------------------------

if 'TENFams' in table_names_array:
    cursor.execute("SELECT Family FROM TENFams")
    query_list = [row[0] for row in cursor.fetchall()]

    for family_entry in query_list:
        query = f'''
        SELECT IPNI{month}.rhakhis_wfo AS WFOID,
               IPNI{month}.id AS IPNID,
               IPNI{month}.taxon_scientific_name_s_lower AS scientificName,
               IPNI{month}.authors_t as authorship,
               IPNI{month}.reference_t AS namePublishedin,
               IPNI{month}.name_status_s_lower AS nomenclaturalStatus,
               IPNI{month}.basionym_s_lower AS originalName,
               IPNI{month}.basionym_author_s_lower AS originalNameAuthor
        FROM IPNI{month}
        WHERE IPNI{month}.family_s_lower = ?
        '''

        cursor.execute(query, (family_entry,))
        results = cursor.fetchall()
        record_count = len(results)

        if record_count > 0:
            column_names = [description[0] for description in cursor.description]
            directory_name = f'IPNInewRecords/2026/{family_entry}'
            os.makedirs(directory_name, exist_ok=True)
            file_name = f'{directory_name}/{family_entry}_{month}.csv'

            with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(column_names)
                writer.writerows(results)

            with open(f'IPNInewRecords/2026/Result_log_{month}.txt', 'a') as log_file:
                log_file.write(f'{family_entry} executed at: {str(time())} - Count: {record_count}\n')

        else:
            with open(f'IPNInewRecords/2026/noResult_log_{month}.txt', 'a') as log_file:
                log_file.write(f'{family_entry} executed at: {str(time())} - Count: 0\n')

# ---------------------------------------------------------
# EXISTING OTHER TEN TABLES
# ---------------------------------------------------------

for table_name in table_names_array:
    if table_name != 'TENFams':
        print(f"Processing table: {table_name}")

        if table_name == "Cichorieae":
            query = f'''
            SELECT IPNI{month}.rhakhis_wfo AS WFOID,
                   IPNI{month}.id AS IPNID,
                   IPNI{month}.taxon_scientific_name_s_lower AS scientificName,
                   IPNI{month}.authors_t as authorship,
                   IPNI{month}.reference_t AS namePublishedin,
                   IPNI{month}.genus_s_lower AS Genus,
                   IPNI{month}.name_status_s_lower AS nomenclaturalStatus,
                   IPNI{month}.basionym_s_lower AS originalName,
                   IPNI{month}.basionym_author_s_lower AS originalNameAuthor
            FROM IPNI{month}
            INNER JOIN {table_name}
            WHERE {table_name}.Genus = IPNI{month}.genus_s_lower;
            '''
        else:
            query = f'''
            SELECT IPNI{month}.rhakhis_wfo AS WFOID,
                   IPNI{month}.id AS IPNID,
                   IPNI{month}.taxon_scientific_name_s_lower AS scientificName,
                   IPNI{month}.authors_t as authorship,
                   IPNI{month}.reference_t AS namePublishedin,
                   IPNI{month}.family_s_lower AS Family,
                   IPNI{month}.name_status_s_lower AS nomenclaturalStatus,
                   IPNI{month}.basionym_s_lower AS originalName,
                   IPNI{month}.basionym_author_s_lower AS originalNameAuthor
            FROM IPNI{month}
            INNER JOIN {table_name}
            WHERE {table_name}.Family = IPNI{month}.family_s_lower;
            '''

        cursor.execute(query)
        results = cursor.fetchall()
        record_count = len(results)

        if record_count > 0:
            print(f"Records found for table: {table_name}, count: {record_count}")
            column_names = [description[0] for description in cursor.description]
            directory_name = f'IPNInewRecords/2026/{table_name}'
            os.makedirs(directory_name, exist_ok=True)
            file_name = f'{directory_name}/{table_name}_{month}.csv'

            with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(column_names)
                writer.writerows(results)

            with open(f'IPNInewRecords/2026/Result_log_{month}.txt', 'a') as log_file:
                log_file.write(f'{table_name} executed at: {str(time())} - Count: {record_count}\n')

        else:
            print(f"No records found for table: {table_name}")
            with open(f'IPNInewRecords/2026/noResult_log_{month}.txt', 'a') as log_file:
                log_file.write(f'{table_name} executed at: {str(time())} - Count: 0\n')

# ---------------------------------------------------------
# NEW SECTION: NOT EXTRACTED RECORDS (with curated columns)
# ---------------------------------------------------------

cursor.execute("DROP TABLE IF EXISTS ExtractedIDs;")
cursor.execute("CREATE TEMP TABLE ExtractedIDs (IPNID TEXT PRIMARY KEY);")

# Insert EuroPlusMed IDs
cursor.executemany(
    "INSERT OR IGNORE INTO ExtractedIDs (IPNID) VALUES (?);",
    [(row[1],) for row in geo_results]
)

# Insert TENFams IDs
if 'TENFams' in table_names_array:
    cursor.execute("SELECT Family FROM TENFams")
    fams = [row[0] for row in cursor.fetchall()]
    for fam in fams:
        cursor.execute(f"SELECT id FROM IPNI{month} WHERE family_s_lower = ?", (fam,))
        ids = cursor.fetchall()
        cursor.executemany("INSERT OR IGNORE INTO ExtractedIDs (IPNID) VALUES (?);", ids)

# Insert other TEN table IDs (respecting special cases)
for table_name in table_names_array:
    if table_name != 'TENFams':

        if table_name == "Cichorieae":
            cursor.execute(
                f"SELECT IPNI{month}.id "
                f"FROM IPNI{month} "
                f"INNER JOIN Cichorieae "
                f"ON Cichorieae.Genus = IPNI{month}.genus_s_lower"
            )
        else:
            cursor.execute(
                f"SELECT IPNI{month}.id "
                f"FROM IPNI{month} "
                f"INNER JOIN {table_name} "
                f"ON {table_name}.Family = IPNI{month}.family_s_lower"
            )

        ids = cursor.fetchall()
        cursor.executemany("INSERT OR IGNORE INTO ExtractedIDs (IPNID) VALUES (?);", ids)

# Now get NOT extracted using the SAME projection as other outputs
not_extracted_query = f"""
SELECT
    rhakhis_wfo AS WFOID,
    id AS IPNID,
    family_s_lower AS Family,
    taxon_scientific_name_s_lower AS scientificName,
    authors_t AS authorship,
    reference_t AS namePublishedin,
    genus_s_lower AS Genus,
    name_status_s_lower AS nomenclaturalStatus,
    basionym_s_lower AS originalName,
    basionym_author_s_lower AS originalNameAuthor
FROM IPNI{month}
WHERE id NOT IN (SELECT IPNID FROM ExtractedIDs);
"""

cursor.execute(not_extracted_query)
not_extracted = cursor.fetchall()
not_extracted_count = len(not_extracted)

# Output directory
missing_dir = "IPNInewRecords/2026/GCT_names"
os.makedirs(missing_dir, exist_ok=True)
missing_file = f"{missing_dir}/gct_names{month}.csv"

if not_extracted_count > 0:
    # Column names match the projection above
    column_names = [
        "WFOID", "IPNID", "scientificName", "authorship",
        "namePublishedin", "Genus", "nomenclaturalStatus",
        "originalName", "originalNameAuthor"
    ]

    with open(missing_file, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(column_names)
        writer.writerows(not_extracted)

    with open(f'IPNInewRecords/2026/Result_log_{month}.txt', 'a') as log_file:
        log_file.write(f'gct executed at: {str(time())} - Count: {not_extracted_count}\n')

else:
    with open(f'IPNInewRecords/2026/noResult_log_{month}.txt', 'a') as log_file:
        log_file.write(f'gct executed at: {str(time())} - Count: 0\n')

# Copy to TEN repo
sourceDir = r"IPNInewRecords/2026"
destinationDir = r"C:\Users\alane\OneDrive\Documents\wfo\wfo-tens\wfo-tens\IPNI\2026"
shutil.copytree(sourceDir, destinationDir, symlinks=False, ignore=None, copy_function=copy2, dirs_exist_ok=True)

# Copy gct folder to General Curatorial Team repo
GCT_source = r"IPNInewRecords/2026/GCT_names"
gct_destination = r"C:\Users\alane\OneDrive\Documents\wfo\wfo-general-curatorial-team\GCT_names"

shutil.copytree(GCT_source, gct_destination, symlinks=False, ignore=None, copy_function=copy2, dirs_exist_ok=True)

conn.close()