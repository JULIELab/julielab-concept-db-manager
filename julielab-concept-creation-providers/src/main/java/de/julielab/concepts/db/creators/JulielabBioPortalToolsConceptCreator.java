package de.julielab.concepts.db.creators;

import static de.julielab.concepts.util.ConfigurationHelper.checkFilesExist;
import static de.julielab.concepts.util.ConfigurationHelper.checkParameters;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import de.julielab.bioportal.ontologies.data.OntologyClass;
import de.julielab.bioportal.util.BioPortalToolUtils;
import de.julielab.concepts.db.core.spi.ConceptCreator;
import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.ConfigurationHelper;
import de.julielab.concepts.util.UncheckedConceptDBManagerException;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetGroup;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

/**
 * <p>
 * Creates {@link ImportConcept} instances from the ontology class output
 * created by the julielab-bioportal-ontology-tools.
 * </p>
 * <p>
 * This class reads a specific JSON format as it is generated by the
 * julielab-bioportal-ontology-tools. The JSON format is centered around the
 * human-readable names of ontology classes and their taxonomical structure. For
 * more information, refer to the link given below.
 * </p>
 * 
 * @author faessler
 * @see https://github.com/JULIELab/julielab-bioportal-ontology-tools
 */
public class JulielabBioPortalToolsConceptCreator implements ConceptCreator {

	private static final Logger log = LoggerFactory.getLogger(JulielabBioPortalToolsConceptCreator.class);

	public static final String NAME = "julielabontologies";
	
	/**
	 * A file or directory pointing to the JSON file(s) containing the extracted
	 * names of ontology classes. The JSON format is required to match the
	 * definition of the julielab-bioportal-ontology-tools.
	 */
	public static final String CONFKEY_ONTOLOGY_CLASS_NAMES = "configuration.path";
	/**
	 * In the Neo4j database, concepts are grouped in facets and facets are grouped
	 * into facet groups. This is the unique name of the facet group the currently
	 * inserted facet should go to.
	 */
	public static final String CONFKEY_FACET_GROUP_NAME = "configuration.facet.facetgroup.name";

	@Override
	public Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> config)
			throws ConceptCreationException {
		checkParameters(config, CONFKEY_FACET_GROUP_NAME, CONFKEY_ONTOLOGY_CLASS_NAMES);
		checkFilesExist(config, CONFKEY_ONTOLOGY_CLASS_NAMES);
		// First, read the configuration.
		String facetGroupName = config.getString(CONFKEY_FACET_GROUP_NAME);
		File ontologyNamesPath = new File(config.getString(CONFKEY_ONTOLOGY_CLASS_NAMES));

		Gson gson = new Gson();
		// First, get all files with class names.
		File[] ontologyNameFiles;
		if (ontologyNamesPath.isDirectory())
			ontologyNameFiles = ontologyNamesPath
					.listFiles((f, n) -> n.endsWith(".jsonlst") || n.endsWith(".jsonlst.gz"));
		else
			ontologyNameFiles = new File[] { ontologyNamesPath };

		log.info("Reading {} ontology class files for concept creation.", ontologyNameFiles.length);
		// The following stream reads the class name files one after the other and
		// creates a facet for each file and the respective concepts. Thus, the
		// assumption is that each file corresponds to one ontology.
		// Also, the file name must be a unique name for the facet.
		return Stream.of(ontologyNameFiles).map(f -> {
			try {
				String acronym = BioPortalToolUtils.getAcronymFromFileName(f);

				// The format of the name files is one class per line as
				// a JSON object on its own. We will now build a JSON
				// array out of all the classes
				// of the file
				BufferedReader br = FileUtilities.getReaderFromFile(f);
				// Convert the JSON lines to OntologyClass objects. However, the OntologyClass
				// class is the representation for ontology classes of the BioPortal tools
				// project. We need to translate this to ImportConcept instances.
				Stream<OntologyClass> classStream = br.lines().map(l -> gson.fromJson(l, OntologyClass.class));
				// Convert the OntologyClass objects to ImportConcepts
				Stream<ImportConcept> conceptStream = classStream.map(c -> {
					// Collect the parents of the current class, if there are any.
					List<ConceptCoordinates> parentCoordinates = Collections.emptyList();
					if (c.parents != null && c.parents.parents != null)
						parentCoordinates = c.parents.parents.stream()
								.map(p -> new ConceptCoordinates(p, acronym, true)).collect(Collectors.toList());
					// Now create the actual concepts that can be imported into the database.
					return new ImportConcept(c.prefLabel, c.synonym.synonyms, c.definition,
							new ConceptCoordinates(c.id, acronym, true), parentCoordinates);
				});
				// Note: Facet groups are unique by name in the database (the
				// ConceptManager that does the concept insertion makes sure of it).
				ImportFacetGroup fg = new ImportFacetGroup(facetGroupName);
				// We use the acronym as name, short name and custom ID. The last shouldn't be a
				// problem since ontology acronyms are unique within BioPortal. There could be
				// an issue with ontologies from outside, but let's just hope for now that there
				// won't. 
				ImportFacet facet = new ImportFacet(fg, acronym, acronym, acronym,
						FacetConstants.SRC_TYPE_HIERARCHICAL);
				return new ImportConcepts(conceptStream, facet);
			} catch (IOException e) {
				throw new UncheckedConceptDBManagerException(new ConceptCreationException(e));
			}
		});
	}

	@Override
	public boolean hasName(String providername) {
		if (providername == null)
			throw new IllegalArgumentException("The concept creation service provider name is null");
		return providername.equals(this.getClass().getName()) || providername.equals(NAME);
	}
}
