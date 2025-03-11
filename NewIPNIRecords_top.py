## Script to split monthly IPNI records once matching into csv's for distribution to TENs. 
## Take the IPNI export add to WFOsqlite.db as IPNI{month}. Search and replace current three letter month for current.
## Script needs updated when new TEN's added. 
## Edits to make - Some multifamily TENs need combined. Commelinales check others.

import sqlite3
import csv
import datetime
import os

# Get the current time
time = datetime.datetime.now

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('data/WFOsqlite.db')

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Caryophyllales file
# Execute the SQL query
cursor.execute('''SELECT IPNIFeb.rhakhis_wfo AS WFOID,
       IPNIFeb.id AS IPNID,
       IPNIFeb.taxon_scientific_name_s_lower AS scientificName,
       IPNIFeb.authors_t,
       IPNIFeb.reference_t AS namePublishedin,
       IPNIFeb.family_s_lower AS Family,
       IPNIFeb.name_status_s_lower AS nomenclaturalStatus,
       IPNIFeb.basionym_s_lower AS originalName,
       IPNIFeb.basionym_author_s_lower AS originalNameAuthor
  FROM IPNIFeb
       INNER JOIN
       CarophyllalesFamilies
 WHERE CarophyllalesFamilies.Family = IPNIFeb.family_s_lower;
  ''')

# Fetch all the results
results = cursor.fetchall()

if results:
        # Get the column names
        column_names = [description[0] for description in cursor.description]

        # Write the results to a CSV file
        with open('newIPNIrecords/Caryophyllales_Feb.csv', 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write the column headers
            csv_writer.writerow(column_names)
            # Write the data rows
            csv_writer.writerows(results)

        print(f"Records returned from Caryophyllales query.")
        # Log the information to a file
        with open('newIPNIrecords/Result_log.txt', 'a') as log_file:
            log_file.write(f'Records returned from the Caryophyllales query executed at: ' + str(time()) + '\n')

else:
        # No records found, log or record this information
        print(f"No records returned from Caryophyllales query.")
        # Log the information to a file
        with open('newIPNIrecords/noResult_log.txt', 'a') as log_file:
            log_file.write(f'No records returned from the Caryophyllales query executed at: ' + str(time()) + '\n')

## PPG Families

# Execute the SQL query
cursor.execute('''SELECT IPNIFeb.rhakhis_wfo AS WFOID,
       IPNIFeb.id AS IPNID,
       IPNIFeb.taxon_scientific_name_s_lower AS scientificName,
       IPNIFeb.authors_t,
       IPNIFeb.reference_t AS namePublishedin,
       IPNIFeb.family_s_lower AS Family,
       IPNIFeb.name_status_s_lower AS nomenclaturalStatus,
       IPNIFeb.basionym_s_lower AS originalName,
       IPNIFeb.basionym_author_s_lower AS originalNameAuthor
  FROM IPNIFeb
       INNER JOIN
       PPGFamilies
 WHERE PPGFamilies.Family = IPNIFeb.family_s_lower;
  ''')

# Fetch all the results
results = cursor.fetchall()

if results:
        # Get the column names
        column_names = [description[0] for description in cursor.description]

        # Write the results to a CSV file
        with open('newIPNIrecords/PPGFamilies_Feb.csv', 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write the column headers
            csv_writer.writerow(column_names)
            # Write the data rows
            csv_writer.writerows(results)

        print(f"Records returned from PPGFamilies query.")
        # Log the information to a file
        with open('newIPNIrecords/Result_log.txt', 'a') as log_file:
            log_file.write(f'Records returned from the PPGFamilies query executed at: ' + str(time()) + '\n')

else:
        # No records found, log or record this information
        print(f"No records returned from PPGFamilies query.")
        # Log the information to a file
        with open('newIPNIrecords/noResult_log.txt', 'a') as log_file:
            log_file.write(f'No records returned from the PPGFamilies query executed at: ' + str(time()) + '\n')

## Conifers Families

# Execute the SQL query
cursor.execute('''SELECT IPNIFeb.rhakhis_wfo AS WFOID,
       IPNIFeb.id AS IPNID,
       IPNIFeb.taxon_scientific_name_s_lower AS scientificName,
       IPNIFeb.authors_t,
       IPNIFeb.reference_t AS namePublishedin,
       IPNIFeb.family_s_lower AS Family,
       IPNIFeb.name_status_s_lower AS nomenclaturalStatus,
       IPNIFeb.basionym_s_lower AS originalName,
       IPNIFeb.basionym_author_s_lower AS originalNameAuthor
  FROM IPNIFeb
       INNER JOIN
       Conifers
 WHERE Conifers.Family = IPNIFeb.family_s_lower;
  ''')

# Fetch all the results
results = cursor.fetchall()

if results:
        # Get the column names
        column_names = [description[0] for description in cursor.description]

        # Write the results to a CSV file
        with open('newIPNIrecords/Conifers_Feb.csv', 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write the column headers
            csv_writer.writerow(column_names)
            # Write the data rows
            csv_writer.writerows(results)

        print(f"Records returned from Conifers families query.")
        # Log the information to a file
        with open('newIPNIrecords/Result_log.txt', 'a') as log_file:
            log_file.write(f'Records returned from the Conifers Families query executed at: ' + str(time()) + '\n')

else:
        # No records found, log or record this information
        print(f"No records returned from Conifers Families query.")
        # Log the information to a file
        with open('newIPNIrecords/noResult_log.txt', 'a') as log_file:
            log_file.write(f'No records returned from the Conifers families query executed at: ' + str(time()) + '\n')

## Cycads Families

# Execute the SQL query
cursor.execute('''SELECT IPNIFeb.rhakhis_wfo AS WFOID,
       IPNIFeb.id AS IPNID,
       IPNIFeb.taxon_scientific_name_s_lower AS scientificName,
       IPNIFeb.authors_t,
       IPNIFeb.reference_t AS namePublishedin,
       IPNIFeb.family_s_lower AS Family,
       IPNIFeb.name_status_s_lower AS nomenclaturalStatus,
       IPNIFeb.basionym_s_lower AS originalName,
       IPNIFeb.basionym_author_s_lower AS originalNameAuthor
  FROM IPNIFeb
       INNER JOIN
       Cycads
 WHERE Cycads.Family = IPNIFeb.family_s_lower;
  ''')

# Fetch all the results
results = cursor.fetchall()

if results:
        # Get the column names
        column_names = [description[0] for description in cursor.description]

        # Write the results to a CSV file
        with open('newIPNIrecords/Cycads_Feb.csv', 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write the column headers
            csv_writer.writerow(column_names)
            # Write the data rows
            csv_writer.writerows(results)

        print(f"Records returned from Cycads query.")
        # Log the information to a file
        with open('newIPNIrecords/Result_log.txt', 'a') as log_file:
            log_file.write(f'Records returned from the Cycads query executed at: ' + str(time()) + '\n')

else:
        # No records found, log or record this information
        print(f"No records returned from Cycads query.")
        # Log the information to a file
        with open('newIPNIrecords/noResult_log.txt', 'a') as log_file:
            log_file.write(f'No records returned from the Cycads query executed at: ' + str(time()) + '\n')

## Haloragaceae Families

# Execute the SQL query
cursor.execute('''SELECT IPNIFeb.rhakhis_wfo AS WFOID,
       IPNIFeb.id AS IPNID,
       IPNIFeb.taxon_scientific_name_s_lower AS scientificName,
       IPNIFeb.authors_t,
       IPNIFeb.reference_t AS namePublishedin,
       IPNIFeb.family_s_lower AS Family,
       IPNIFeb.name_status_s_lower AS nomenclaturalStatus,
       IPNIFeb.basionym_s_lower AS originalName,
       IPNIFeb.basionym_author_s_lower AS originalNameAuthor
  FROM IPNIFeb
       INNER JOIN
       Haloragaceae
 WHERE Haloragaceae.Family = IPNIFeb.family_s_lower;
  ''')

# Fetch all the results
results = cursor.fetchall()

if results:
        # Get the column names
        column_names = [description[0] for description in cursor.description]

        # Write the results to a CSV file
        with open('newIPNIrecords/Haloragaceae_Feb.csv', 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write the column headers
            csv_writer.writerow(column_names)
            # Write the data rows
            csv_writer.writerows(results)

        print(f"Records returned from Haloragaceae and sister families query.")
        # Log the information to a file
        with open('newIPNIrecords/Result_log.txt', 'a') as log_file:
            log_file.write(f'Records returned from the Haloragaceae and sister Families query executed at: ' + str(time()) + '\n')

else:
        # No records found, log or record this information
        print(f"No records returned from Haloragaceae and sister Families query.")
        # Log the information to a file
        with open('newIPNIrecords/noResult_log.txt', 'a') as log_file:
            log_file.write(f'No records returned from the Haloragaceae and sister families query executed at: ' + str(time()) + '\n')

## Legumes - Fabaceae, Leguminosae, Caesalpiniaceae names used in IPNI

# Execute the SQL query
cursor.execute('''SELECT IPNIFeb.rhakhis_wfo AS WFOID,
       IPNIFeb.id AS IPNID,
       IPNIFeb.taxon_scientific_name_s_lower AS scientificName,
       IPNIFeb.authors_t,
       IPNIFeb.reference_t AS namePublishedin,
       IPNIFeb.family_s_lower AS Family,
       IPNIFeb.name_status_s_lower AS nomenclaturalStatus,
       IPNIFeb.basionym_s_lower AS originalName,
       IPNIFeb.basionym_author_s_lower AS originalNameAuthor
  FROM IPNIFeb
       INNER JOIN
       Legumes
 WHERE Legumes.Family = IPNIFeb.family_s_lower;
  ''')

# Fetch all the results
results = cursor.fetchall()

if results:
        # Get the column names
        column_names = [description[0] for description in cursor.description]

        # Write the results to a CSV file
        with open('newIPNIrecords/Legumes_Feb.csv', 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write the column headers
            csv_writer.writerow(column_names)
            # Write the data rows
            csv_writer.writerows(results)

        print(f"Records returned from Legume families query.")
        # Log the information to a file
        with open('newIPNIrecords/Result_log.txt', 'a') as log_file:
            log_file.write(f'Records returned from the Legume Families query executed at: ' + str(time()) + '\n')

else:
        # No records found, log or record this information
        print(f"No records returned from Legumes Families query.")
        # Log the information to a file
        with open('newIPNIrecords/noResult_log.txt', 'a') as log_file:
            log_file.write(f'No records returned from the Legume families query executed at: ' + str(time()) + '\n')

## Ten Families working on one group

# Step 1: Retrieve the family list from the PPG Family List
cursor.execute('''SELECT Family FROM TENFams''')
query_list = [row[0] for row in cursor.fetchall()]

# Step 2: Loop through the family and execute an SQL query for each value
for family_entry in query_list:
    # Execute the SQL query
    cursor.execute('''SELECT IPNIFeb.rhakhis_wfo AS WFOID,
        IPNIFeb.id AS IPNID,
        IPNIFeb.taxon_scientific_name_s_lower AS scientificName,
        IPNIFeb.authors_t,
        IPNIFeb.reference_t AS namePublishedin,
        IPNIFeb.name_status_s_lower AS nomenclaturalStatus,
        IPNIFeb.basionym_s_lower AS originalName,
        IPNIFeb.basionym_author_s_lower AS originalNameAuthor
    FROM IPNIFeb WHERE IPNIFeb.family_s_lower = ?''', (family_entry,))

    # Fetch all the results
    results = cursor.fetchall()
    
    if results:
        # Get the column names
        column_names = [description[0] for description in cursor.description]

        #create the directory name
        directory_name = f'newIPNIrecords/{family_entry}'

        os.makedirs(directory_name, exist_ok=True)
        # Create the file name based on the entry
        file_name = f'newIPNIrecords/{family_entry}/{family_entry}_Feb.csv'
        print(file_name)

        # Write the results to a CSV file
        with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write the column headers
            csv_writer.writerow(column_names)
            # Write the data rows
            csv_writer.writerows(results)
        #not records found
        print(f"Records returned from {family_entry} query.")
        # Log the information to a file
        with open('newIPNIrecords/Result_log.txt', 'a') as log_file:
            log_file.write(f'Records returned from the {family_entry} query executed at: ' + str(time()) + '\n')

    else:
        # No records found, log or record this information
        print(f"No records returned from {family_entry} query.")
        # Log the information to a file
        with open('newIPNIrecords/noResult_log.txt', 'a') as log_file:
            log_file.write(f'No records returned from the {family_entry} query executed at: ' + str(time()) + '\n')
# Close the connection
conn.close()