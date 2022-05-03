# HGNC Gene Groups Concept Creator

Imports the HGNC group/family hierarchy into Neo4j in the JULIE Lab concept format. Links to NCBI Gene IDs are created. To actually import those genes, use the `julielab-concept-creation-ncbi-gene` module.

## Obtain the input files

The `family.csv`, `family_alias.csv` and `hierarchy.csv` files can be downloaded from <http://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/csv/genefamily_db_tables/>.

We do not use the `gene_has_family.csv` file because we need a map to NCBI Genes anyway. Thus, we use the custom download to obtain the HGNC gene to HGNC group and NCBI Gene ID map via
```
https://www.genenames.org/cgi-bin/download/custom?col=gd_hgnc_id&col=md_eg_id&col=family.id&col=gd_pub_eg_id&status=Approved&status=Entry%20Withdrawn&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit
```