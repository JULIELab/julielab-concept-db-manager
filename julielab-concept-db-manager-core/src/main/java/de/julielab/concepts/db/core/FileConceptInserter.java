package de.julielab.concepts.db.core;

import java.net.URISyntaxException;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.shell.util.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import de.julielab.concepts.db.core.services.FileDatabaseService;
import de.julielab.concepts.db.core.spi.ConceptInserter;
import de.julielab.concepts.util.ConceptDatabaseCreationException;
import de.julielab.concepts.util.ConceptInsertionException;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

public class FileConceptInserter implements ConceptInserter {

	private static final Logger log = LoggerFactory.getLogger(FileConceptInserter.class);
	private GraphDatabaseService graphDb;
	private FileDatabaseService fileDbService;
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;

	public FileConceptInserter() {
		fileDbService = FileDatabaseService.getInstance();
	}

	@Override
	public void insertConcepts(ImportConcepts concepts) throws ConceptInsertionException {
		if (graphDb == null)
			throw new ConceptInsertionException(
					"No access to a file-based graph database. The FileConceptInserter has not been initialized properly. Call setConfiguration() and check its return value before calling this method.");
		ConceptManager cm = new ConceptManager();
		log.info("Inserting concepts into embedded Neo4j database at {}.", connectionConfiguration.getString("uri"));
		ImportFacet facet = concepts.getFacet();
		if (facet == null)
			throw new ConceptInsertionException("The facet of the import concepts is null.");
		String customId = facet.getCustomId();
		ObjectMapper jsonMapper = new ObjectMapper().registerModule(new Jdk8Module());
		jsonMapper.setSerializationInclusion(Include.NON_NULL);
		jsonMapper.setSerializationInclusion(Include.NON_EMPTY);
		boolean alreadyExists = false;
		try (Transaction tx = graphDb.beginTx()) {
			Node facetNode = graphDb.findNode(FacetLabel.FACET, FacetConstants.PROP_CUSTOM_ID, customId);
			alreadyExists = facetNode != null;
			tx.success();
		}
		if (!alreadyExists) {
			try {
				log.trace("Inserting the concepts of facet {} (customId: {}) into the Neo4j database", facet.getName(),
						facet.getCustomId());
				String conceptsJson = jsonMapper.writeValueAsString(concepts);
				cm.insertConcepts(graphDb, conceptsJson);
			} catch (JSONException e) {
				throw new ConceptInsertionException(
						"The JSON format specifying the ontology class names or - but less probable - the facet JSON format does not fit the requirements of the employed version of the julielab-neo4j-plugin-concepts dependency. There might be a compatibility issue between the julielab-bioportal-tools and the plugin-concepts libraries.",
						e);
			} catch (JsonProcessingException e) {
				throw new ConceptInsertionException(e);
			}
		} else {
			// ontology facet node was found
			log.debug("Facet with cutom ID {} already exists in the database and is not inserted again.",
					facet.getCustomId());
		}
	}

	@Override
	public boolean setConfiguration(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
			throws URISyntaxException, ConceptDatabaseCreationException {
		this.connectionConfiguration = connectionConfiguration;
		graphDb = fileDbService.getDatabase(connectionConfiguration);
		return graphDb != null;
	}

}
