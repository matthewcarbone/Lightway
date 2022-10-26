# Ingestion steps

- for all comment lines, if they contain a colon, remove starting hash and split at colon to create key-value pairs
- the only comment line without colon should be the header for DataFrame
- place key-value pairs in metadata dictionary
- use data in .dat file to calculate mu_trans, mu_fluor, and mu_ref using respective formulas
