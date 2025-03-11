-- SQLite
SELECT IPNIJan.rhakhis_wfo AS WFOID,
       IPNIJan.id AS IPNID,
       IPNIJan.taxon_scientific_name_s_lower AS scientificName,
       IPNIJan.authors_t,
       IPNIJan.reference_t AS namePublishedin,
       IPNIJan.name_status_s_lower AS nomenclaturalStatus,
       IPNIJan.basionym_s_lower AS originalName,
       IPNIJan.basionym_author_s_lower AS originalNameAuthor
  FROM IPNIJan
       INNER JOIN
       CarophyllalesFamilies
 WHERE CarophyllalesFamilies.Family = IPNIJan.family_s_lower;
 