# Ingestion steps

.dat file contains commented lines (denoted by #) with metadata, and columnated data beneath metadata.

- for all comment lines, if they contain a colon, remove starting hash and split at colon to create key-value pairs
- the only comment line without colon should be the header for columnated data
- place key-value pairs in metadata dictionary
- columnated data should contain energy, i0, it, ir, iff, and aux channels
- use data in .dat file to calculate mu_trans, mu_fluor, and mu_ref, i.e:
```
mu_trans = -np.log(it/i0)
mu_fluor = iff/i0
mu_ref = -np.log(ir/i0)
```
- final DataFrame contains energy, mu_trans, mu_fluor, and mu_ref