#!/bin/bash
# This is a utility script to update the XML schemas for the concept DB manager configuration.
# To make use of this, access to the JULIE Lab GitHub page is required. The respective repository must be
# cloned and its path given as the third parameter.

if [ "$#" -ne 1 ]; then
    echo "Usage: deployXmlSchemas.sh <julie lab gitpage dir>"
    exit 1
fi

julielabgitpagedir=$1
schema1_source=julielab-concept-db-manager-core/src/main/resources/conceptdb-*.xsd
schema2_source=julielab-concept-db-manager-core/src/main/resources/defaultfacet-*.xsd
schema3_source=julielab-concept-db-manager-core/src/main/resources/exporter-*.xsd
schema4_source=julielab-concept-db-manager-core/src/main/resources/methodcallbase-*.xsd
schema5_source=julielab-concept-db-manager-core/src/main/resources/operation-*.xsd
schema6_source=julielab-concept-creation-ncbi-gene/src/main/resources/ncbigeneconcepts-*.xsd
schema7_source=julielab-concept-creation-bioportal/src/main/resources/bioportalconcepts-*.xsd
schema8_source=julielab-concept-creation-bioportal/src/main/resources/bioportalfacet-*.xsd
schema9_source=julielab-concept-creation-bioportal/src/main/resources/bioportalmappings-*.xsd
schema10_source=julielab-concept-creation-mesh/src/main/resources/xmlconcepts-*.xsd
schema11_source=julielab-concept-creation-mesh/src/main/resources/meshfacet-*.xsd
schema12_source=julielab-concept-creation-mesh/src/main/resources/simplexmlfacet-*.xsd

schema1_target=$julielabgitpagedir/conceptdb/
schema2_target=$julielabgitpagedir/conceptdb/facets/
schema3_target=$julielabgitpagedir/conceptdb/
schema4_target=$julielabgitpagedir/conceptdb/
schema5_target=$julielabgitpagedir/conceptdb/
schema6_target=$julielabgitpagedir/conceptdb/concepts/
schema7_target=$julielabgitpagedir/conceptdb/concepts/
schema8_target=$julielabgitpagedir/conceptdb/facets/
schema9_target=$julielabgitpagedir/conceptdb/mappings/
schema10_target=$julielabgitpagedir/conceptdb/concepts/
schema11_target=$julielabgitpagedir/conceptdb/facets/
schema12_target=$julielabgitpagedir/conceptdb/facets/
for i in {1..12}; do
	fromname=schema${i}_source
	toname=schema${i}_target
	from=${!fromname}
	to=${!toname}
	cp $from $to
done
