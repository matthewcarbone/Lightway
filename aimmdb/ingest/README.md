Ingestion steps:<br/>
for all comment lines, if they contain a colon, remove starting hash and split at colon to create key-value pairs<br/>
only comment line without colon should be header for DataFrame<br/>
place key-value pairs in metadata dictionary<br/>
use data in file to calculate mu_trans, mu_fluor, and mu_ref using respective formulas<br/>
