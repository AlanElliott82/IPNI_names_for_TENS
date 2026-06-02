import sqlite3
import csv
import datetime
import os

# Timestamp
time = datetime.datetime.now

# Connect to DB
conn = sqlite3.connect('data/WFOsqlite.db')
cursor = conn.cursor()

# Month to run (change each run)
month = "April26"   # Jul25, Aug25, Sep25, Oct25, Nov25, Dec25

# ---------------------------------------------------------
# CMEP Geography Matching
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
    JOIN CMEP_Geography AS g
      ON TRIM(SUBSTR(i.distribution_s_lower, 1, INSTR(i.distribution_s_lower, '(') - 1))
         = LOWER(g.Country)

    UNION

    SELECT i.*
    FROM IPNI{month} AS i
    JOIN CMEP_TDWG AS g
      ON i.distribution_s_lower LIKE '%' || LOWER(g.Region) || '%'
);
"""

cursor.execute(geo_query)
geo_results = cursor.fetchall()
geo_count = len(geo_results)

# Output directory for 2025 backfill
geo_dir = f'IPNInewRecords/2026/CMEP'
os.makedirs(geo_dir, exist_ok=True)

geo_file = f'{geo_dir}/CMEP_{month}.csv'

# Write results
if geo_count > 0:
    column_names = [description[0] for description in cursor.description]

    with open(geo_file, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(column_names)
        writer.writerows(geo_results)

    with open(f'IPNInewRecords/2026/Result_log_{month}.txt', 'a') as log_file:
        log_file.write(f'CMEP match executed at: {str(time())} - Count: {geo_count}\n')

else:
    with open(f'IPNInewRecords/2026/noResult_log_{month}.txt', 'a') as log_file:
        log_file.write(f'CMEP match executed at: {str(time())} - Count: 0\n')