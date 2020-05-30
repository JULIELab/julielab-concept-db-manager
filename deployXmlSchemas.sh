#!/bin/bash
# This is a utility script to update the XML schemas for the concept DB manager configuration.
# To make use of this, access to the JULIE Lab GitHub page is required. The respective repository must be
# cloned and its path given as the third parameter.

if [ "$#" -ne 3 ]; then
    echo "Usage: deployXmlSchemas.sh <version> <override 0/1> <julie lab gitpage dir>"
    exit 1
fi

version=$1
override=$2
julielabgitpagedir=$3
if [ ! -z $version ]; then
	if [ -z $override ]; then override=0; fi
	schema1_source=julielab-concept-db-manager-core/src/main/resources/conceptdb.xsd
	schema2_source=julielab-concept-db-manager-core/src/main/resources/defaultfacet.xsd
	schema3_source=julielab-concept-db-manager-core/src/main/resources/exporter.xsd
	schema4_source=julielab-concept-db-manager-core/src/main/resources/methodcallbase.xsd
	schema5_source=julielab-concept-db-manager-core/src/main/resources/operation.xsd
	schema6_source=julielab-concept-creation-ncbi-gene/src/main/resources/ncbigeneconcepts.xsd
	schema7_source=julielab-concept-creation-bioportal/src/main/resources/bioportalconcepts.xsd
	schema8_source=julielab-concept-creation-bioportal/src/main/resources/bioportalfacet.xsd
	schema9_source=julielab-concept-creation-bioportal/src/main/resources/bioportalmappings.xsd
	schema10_source=julielab-concept-creation-mesh/src/main/resources/xmlconcepts.xsd
	schema11_source=julielab-concept-creation-mesh/src/main/resources/meshfacet.xsd
	schema12_source=julielab-concept-creation-mesh/src/main/resources/simplexmlfacet.xsd

	schema1_target=$julielabgitpagedir/conceptdb/conceptdb-$version.xsd
	schema2_target=$julielabgitpagedir/conceptdb/facets/defaultfacet-$version.xsd
	schema3_target=$julielabgitpagedir/conceptdb/exporter-$version.xsd
	schema4_target=$julielabgitpagedir/conceptdb/methodcallbase-$version.xsd
	schema5_target=$julielabgitpagedir/conceptdb/operation-$version.xsd
	schema6_target=$julielabgitpagedir/conceptdb/concepts/ncbigeneconcepts-$version.xsd
	schema7_target=$julielabgitpagedir/conceptdb/concepts/bioportalconcepts-$version.xsd
	schema8_target=$julielabgitpagedir/conceptdb/facets/bioportalfacet-$version.xsd
	schema9_target=$julielabgitpagedir/conceptdb/mappings/bioportalmappings-$version.xsd
	schema10_target=$julielabgitpagedir/conceptdb/concepts/xmlconcepts-$version.xsd
	schema11_target=$julielabgitpagedir/conceptdb/facets/meshfacet-$version.xsd
	schema12_target=$julielabgitpagedir/conceptdb/facets/simplexmlfacet-$version.xsd
	for i in {1..12}; do
		fromname=schema${i}_source
		toname=schema${i}_target
		from=${!fromname}
		to=${!toname}
		if [ ! -f "$to" ]; then
			echo "Writing schema $from to $to"
			cp $from $to
		elif [ 1 -eq "$override" ]; then
			echo "File $to already exists. It is overwritten."
			cp $from $to
		else
			echo "File $to already exists and is not overwritten."
		fi
	done
else
	echo "A schema version is required."
fi