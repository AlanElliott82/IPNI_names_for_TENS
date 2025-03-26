# IPNI_names_for_TENS
Workflow to batch process and create files for TENs in WFO from IPNI workflow

At the beginning of each month WFO added newly added names to the IPNI dataset, this averages 500-700 names per month. 

To better distribute to Taxonomic Expert Networks we are now pushing these to the WFO GitHub data repository. Each month there will be a new folder with a csv for the groups a TEN is responsible for usually Families. There will also be a log that records which groups had names and which did not.

The basic csv will contain the WFO ID, the IPNI ID, Scientific name, Authorship, place of publication and if there is a basionym for that name.
If TEN would like additional data please just let us know and we can tailor the export to what you need.

Second workflow extracts any doi's from the data and adds these to the WFO Zotero Library.