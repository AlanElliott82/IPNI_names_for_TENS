## Script to split monthly IPNI records once matching into csv's for distribution to TENs. 
## Take the IPNI export add to WFOsqlite.db as IPNI{month}. Change month variable to current three letter.
## Adding a new TEN to the TENFam table or Adding a new multifamily table shouldn't effect the outputs.
## Partial TEN get the full family releases.
## Each month has a result/no result log.


import sqlite3
import csv
import datetime
import os
import shutil
from shutil import copy2  # Import the copy2 function

# Get the current time
time = datetime.datetime.now

# Connect to the SQLite database
conn = sqlite3.connect('data\WFOsqlite.db')

# Create a cursor object
cursor = conn.cursor()

# Execute a query to get the table names to generate
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# Fetch all results from the executed query
table_names = cursor.fetchall()
# Specify substrings to exclude
exclude_substrings = ['IPNI']
# Extract table names from the fetched result, excluding unwanted names
table_names_array = [table[0] for table in table_names if not any(sub in table[0] for sub in exclude_substrings)]
month = "Feb"
# Ten Families working on one group
if 'TENFams' in table_names_array:
    # Step 1: Retrieve the family list from the TEN Family List
    cursor.execute("SELECT Family FROM TENFams")
    query_list = [row[0] for row in cursor.fetchall()]

    # Step 2: Loop through the family and execute an SQL query for each value
    for family_entry in query_list:
        query = f'''SELECT IPNI{month}.rhakhis_wfo AS WFOID,
            IPNI{month}.id AS IPNID,
            IPNI{month}.taxon_scientific_name_s_lower AS scientificName,
            IPNI{month}.authors_t as authorship,
            IPNI{month}.reference_t AS namePublishedin,
            IPNI{month}.name_status_s_lower AS nomenclaturalStatus,
            IPNI{month}.basionym_s_lower AS originalName,
            IPNI{month}.basionym_author_s_lower AS originalNameAuthor
        FROM IPNI{month} WHERE IPNI{month}.family_s_lower = ?'''
        
        # Execute the SQL query
        cursor.execute(query, (family_entry,))

        # Fetch all the results
        results = cursor.fetchall()

        # Count the number of records returned
        record_count = len(results)

        if record_count > 0:
            # Get the column names
            column_names = [description[0] for description in cursor.description]

            # Create the directory name
            directory_name = f'IPNInewRecords/2025/{family_entry}'
            os.makedirs(directory_name, exist_ok=True)

            # Create the file name based on the entry
            file_name = f'IPNInewRecords/2025/{family_entry}/{family_entry}_{month}.csv'
            #print(file_name)

            # Write the results to a CSV file
            with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.writer(csv_file)
                # Write the column headers
                csv_writer.writerow(column_names)
                # Write the data rows
                csv_writer.writerows(results)
            
            #print(f"Records returned from {family_entry} query.")
            
            # Log the information to a file
            with open(f'IPNInewRecords/2025/Result_log_{month}.txt', 'a') as log_file:
                log_file.write(f'Records returned from the {family_entry} query executed at: {str(time())} - Count: {record_count}\n')
        
        else:
            # No records found, log or record this information
            #print(f"No records returned from {family_entry} query.")
            
            # Log the information to a file
            with open(f'IPNInewRecords/2025/noResult_log_{month}.txt', 'a') as log_file:
                log_file.write(f'No records returned from the {family_entry} query executed at: {str(time())} - Count: {record_count}\n')
# Work through all other tables
for table_name in table_names_array:
    if table_name != 'TENFams':
        print(f"Processing table: {table_name}")  # Debugging log

        # Handle `Cichorieae` table separately with genus linking
        if table_name == "Cichorieae":
            query = f'''SELECT IPNI{month}.rhakhis_wfo AS WFOID,
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
            WHERE {table_name}.Genus = IPNI{month}.genus_s_lower;'''
        else:
            # Default case for other tables linking on family
            query = f'''SELECT IPNI{month}.rhakhis_wfo AS WFOID,
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
            WHERE {table_name}.Family = IPNI{month}.family_s_lower;'''

        # Execute the query
        cursor.execute(query)
        results = cursor.fetchall()

        # Count the number of records returned
        record_count = len(results)

        if record_count > 0:
            print(f"Records found for table: {table_name}, count: {record_count}")  # Debugging log
            column_names = [description[0] for description in cursor.description]
            directory_name = f'IPNInewRecords/2025/{table_name}'
            os.makedirs(directory_name, exist_ok=True)
            file_name = f'IPNInewRecords/2025/{table_name}/{table_name}_{month}.csv'

            with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(column_names)
                csv_writer.writerows(results)

            with open(f'IPNInewRecords/2025/Result_log_{month}.txt', 'a') as log_file:
                log_file.write(f'Records returned from the {table_name} query executed at: {str(time())} - Count: {record_count}\n')
        else:
            print(f"No records found for table: {table_name}")  # Debugging log
            with open(f'IPNInewRecords/2025/noResult_log_{month}.txt', 'a') as log_file:
                log_file.write(f'No records returned from the {table_name} query executed at: {str(time())} - Count: {record_count}\n')


#copy New name files to TEN linked repo.
sourceDir = r"IPNInewRecords/2025"
destinationDir = r"C:\Users\alane\OneDrive\Documents\wfo\wfo-tens\wfo-tens\IPNI\2025"
shutil.copytree(sourceDir, destinationDir, symlinks=False, ignore=None, copy_function=copy2, ignore_dangling_symlinks=False, dirs_exist_ok=True)

# Close the connection
conn.close()