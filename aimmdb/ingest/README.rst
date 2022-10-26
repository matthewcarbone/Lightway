Ingestion steps
---------------

.dat file contains commented lines (denoted by #) with metadata, and columnated data beneath metadata.

- for all comment lines, if they contain a colon, remove starting hash and split at colon to create key-value pairs
- the only comment line without colon should be the header for columnated data
- place key-value pairs in metadata dictionary
- use data in .dat file to calculate mu_trans, mu_fluor, and mu_ref, i.e:
.. math::
    - mu_{trans} = -ln(it/i0)
    - mu_{fluor} = iff/i0
    - mu_{ref} = -ln(ir/i0)
- final DataFrame contains energy, mu_trans, mu_fluor, and mu_ref