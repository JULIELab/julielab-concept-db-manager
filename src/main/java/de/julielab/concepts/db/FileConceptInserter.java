package de.julielab.concepts.db;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.shell.util.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

public class FileConceptInserter implements ConceptInserter {

	private static final Logger log = LoggerFactory.getLogger(FileConceptInserter.class);
	private GraphDatabaseService graphDb;

	public FileConceptInserter(FileDatabaseService databaseService) {
		this.graphDb = databaseService.getFileDatabase();
	}

	@Override
	public void insertConcepts(ImportConcepts concepts) throws ConceptInsertionException {
		Gson gson = new Gson();
		ConceptManager cm = new ConceptManager();
		log.info("Inserting classes into the embedded Neo4j database at {}.");
		ImportFacet facet = concepts.getFacet();
		String customId = facet.getCustomId();
		boolean alreadyExists = false;
		try (Transaction tx = graphDb.beginTx()) {
			Node facetNode = graphDb.findNode(FacetLabel.FACET, FacetConstants.PROP_CUSTOM_ID, customId);
			alreadyExists = facetNode != null;
			tx.success();
		}
		if (!alreadyExists) {
			try {
				log.trace("Inserting the classes of facet {} (customId: {}) into the Neo4j database", facet.getName(),
						facet.getCustomId());
				String conceptsJson = gson.toJson(concepts);
				cm.insertConcepts(graphDb, conceptsJson);
			} catch (JSONException e) {
				throw new ConceptInsertionException(
						"The JSON format specifying the ontology class names or - but less probable - the facet JSON format does not fit the requirements of the employed version of the julielab-neo4j-plugin-concepts dependency. There might be a compatibility issue between the julielab-bioportal-tools and the plugin-concepts libraries.",
						e);
			}
		} else {
			// ontology facet node was found
			log.debug("Facet with cutom ID {} already exists in the database and is not inserted again.",
					facet.getCustomId());
		}
	}

}
