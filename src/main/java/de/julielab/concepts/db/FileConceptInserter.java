package de.julielab.concepts.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.shell.util.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSerializer;

import de.julielab.bioportal.ontologies.data.OntologyClass;
import de.julielab.bioportal.util.BioPortalToolUtils;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetGroup;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

public class FileConceptInserter implements ConceptInserter {
	
	private static final Logger log = LoggerFactory.getLogger(FileConceptInserter.class);
	private GraphDatabaseService graphDb;
	
	public FileConceptInserter(GraphDatabaseService graphDb) {
		this.graphDb = graphDb;
	}
	
	@Override
	public void insertConcepts(ImportConcepts concepts,
			HierarchicalConfiguration<ImmutableNode> insertConfiguration) {
		Gson gson = new Gson();
		ConceptManager cm = new ConceptManager();
		File[] ontologyNameFiles = ontologyNamesDirectory
				.listFiles((f, n) -> n.endsWith(".jsonlst") || n.endsWith(".jsonlst.gz"));
		// INSERT ONTOLOGY CLASSES INTO GRAPH DB
		log.info("Inserting {} ontology class files into the embedded Neo4j database.", ontologyNameFiles.length);
		for (int i = 0; i < ontologyNameFiles.length; i++) {
			File f = ontologyNameFiles[i];
			String acronym = BioPortalToolUtils.getAcronymFromFileName(f);
			boolean alreadyExists = false;
			try (Transaction tx = graphDb.beginTx()) {
				Node facetNode = graphDb.findNode(FacetLabel.FACET, FacetConstants.PROP_NAME, acronym);
				alreadyExists = facetNode != null;
				tx.success();
			}
			if (!alreadyExists) {
				try {
					log.trace("Inserting the classes of file {} into the Neo4j database", f);
					// The format of the name files is one class per line as
					// a JSON object on its own. We will now build a JSON
					// array out of all the classes
					// of the file
					BufferedReader br = FileUtilities.getReaderFromFile(f);
					// Convert the JSON lines to OntologyClass objects
					Stream<OntologyClass> classStream = br.lines().map(l -> gson.fromJson(l, OntologyClass.class));
					// Convert the OntologyClass objects to ImportConcepts
					Stream<ImportConcept> conceptStream = classStream.map(c -> {
						List<ConceptCoordinates> parentCoordinates = Collections.emptyList();
						if (c.parents != null && c.parents.parents != null)
							parentCoordinates = c.parents.parents.stream()
									.map(p -> new ConceptCoordinates(p, acronym, true)).collect(Collectors.toList());
						return new ImportConcept(c.prefLabel, c.synonym.synonyms, c.definition,
								new ConceptCoordinates(c.id, acronym, true), parentCoordinates);
					});
					List<ImportConcept> concepts = conceptStream.collect(Collectors.toList());
					String termsJson = JsonSerializer.toJson(concepts);
					// Facet groups are unique by name in the database (the
					// ConceptManager makes sure of it). Thus, we will have
					// a single facet group with the following name after
					// the import
					// of all ontology classes.
					ImportFacetGroup fg = new ImportFacetGroup("BioPortal Ontologies");
					ImportFacet facet = new ImportFacet(BioPortalToolUtils.getAcronymFromFileName(f), "go",
							FacetConstants.SRC_TYPE_HIERARCHICAL, Arrays.asList("none"), Arrays.asList("none"), 0,
							Arrays.asList("none"), fg);

					String facetJson = JsonSerializer.toJson(facet);
					cm.insertConcepts(graphDb, facetJson, termsJson, null);
				} catch (IOException e) {
					throw new ConceptInsertionException(
							"The ontology name file " + f.getAbsolutePath() + " could not be read", e);
				} catch (JSONException e) {
					throw new ConceptInsertionException(
							"The JSON format specifying the ontology class names or - but less probable - the facet JSON format does not fit the requirements of the employed version of the julielab-neo4j-plugin-concepts dependency. There might be a compatibility issue between the julielab-bioportal-tools and the plugin-concepts libraries.",
							e);
				}
			} else {
				// ontology facet node was found
				log.debug("Ontology with ID {} already exists in the database and is not inserted again.", acronym);
			}
		}
	}



	
}
