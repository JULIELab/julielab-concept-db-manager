package de.julielab.concepts.db.creators.mesh;

import de.julielab.concepts.db.core.services.ConceptInsertionService;
import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.FacetManager;
import de.julielab.neo4j.plugins.concepts.ConceptEdgeTypes;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetGroupConstants;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;
import org.assertj.core.util.Arrays;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static de.julielab.neo4j.plugins.concepts.ConceptLabel.CONCEPT;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertNotNull;

public class XmlConceptCreatorTest {
    private static final File TEST_DB = new File("src/test/resources/graph.db");
    private static Logger log = LoggerFactory.getLogger(XmlConceptCreatorTest.class);

    @AfterTest
    @BeforeTest
    public void afterTest() throws IOException {
        FileUtils.deleteQuietly(TEST_DB);
    }

    @Test
    public void testImportMesh() throws Exception {
        XMLConfiguration xmlConfiguration = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/meshSnippetImportConfig.xml"));
        HierarchicalConfiguration<ImmutableNode> connectionConfiguration = xmlConfiguration.configurationAt(CONNECTION);
        ConceptInsertionService insertionService = ConceptInsertionService.getInstance(connectionConfiguration);
        XmlConceptCreator xmlConceptCreator = new XmlConceptCreator();
        HierarchicalConfiguration<ImmutableNode> importConfig = xmlConfiguration.configurationAt(slash(IMPORTS, IMPORT));
        Stream<ImportConcepts> concepts = xmlConceptCreator.createConcepts(importConfig);
        for (ImportConcepts ic : concepts.collect(toCollection(ArrayList::new))) {
            insertionService.insertConcepts(importConfig, ic);
        }

        FileConnectionService fileConnectionService = FileConnectionService.getInstance();
        GraphDatabaseService graphdb = fileConnectionService.getDatabase(connectionConfiguration);
        // Tests for the Anatomy concepts in the test data
        try (Transaction tx = graphdb.beginTx()) {
            // The snippet contains - amongst others - the concept for "body parts" and some of its subclasses
            Node bodyRegions = tx.findNode(CONCEPT, PROP_ORG_ID, "D001829");
            assertNotNull(bodyRegions);
            Iterable<Relationship> relationships = bodyRegions.getRelationships(Direction.OUTGOING, ConceptEdgeTypes.IS_BROADER_THAN);
            assertThat(relationships).extracting(r -> r.getEndNode()).extracting(n -> n.getProperty(PROP_ORG_ID)).containsExactlyInAnyOrder("D005121", "D006257", "D009333");
            assertThat(relationships).extracting(r -> r.getEndNode()).extracting(n -> n.getProperty(PROP_SOURCES)).flatExtracting(a -> Arrays.asList(a)).containsExactlyInAnyOrder("MeSH XML", "MeSH XML", "MeSH XML");
            assertThat(relationships).extracting(r -> r.getEndNode()).extracting(n -> n.getProperty(ConceptConstants.PROP_ORG_SRC)).containsExactlyInAnyOrder("MeSH XML", "MeSH XML", "MeSH XML");

            Node facet = bodyRegions.getSingleRelationship(ConceptEdgeTypes.HAS_ROOT_CONCEPT, Direction.INCOMING).getStartNode();
            assertThat(facet.getProperty(FacetConstants.PROP_NAME)).isEqualTo("Anatomy");

            Node facetGroup = facet.getSingleRelationship(FacetManager.EdgeTypes.HAS_FACET, Direction.INCOMING).getStartNode();
            // We have configured the facet group name in the configuration
            assertThat(facetGroup.getProperty(FacetGroupConstants.PROP_NAME)).isEqualTo("MeSH");
            tx.commit();
        }

        // Tests for the Organisms concepts in the test data
        try (Transaction tx = graphdb.beginTx()) {
            Node organisms = tx.findNode(FacetManager.FacetLabel.FACET, FacetConstants.PROP_NAME, "Organisms");
            assertThat(organisms).isNotNull();
            Iterable<Relationship> relationships = organisms.getRelationships(Direction.OUTGOING, ConceptEdgeTypes.HAS_ROOT_CONCEPT);
            assertThat(relationships).extracting(r -> r.getEndNode()).extracting(n -> n.getProperty(PROP_ORG_ID)).containsExactlyInAnyOrder("D056890", "D001105");

            Node facetGroup = organisms.getSingleRelationship(FacetManager.EdgeTypes.HAS_FACET, Direction.INCOMING).getStartNode();
            // This facet, too, should belong to the MeSH facet group according to our test configuration
            assertThat(facetGroup.getProperty(FacetGroupConstants.PROP_NAME)).isEqualTo("MeSH");

            tx.commit();
        }

        // Test that the MeSH concepts have their expected properties; we test in on one example, the Eukariota
        try (Transaction tx = graphdb.beginTx()) {
            Node eukaryota = tx.findNode(CONCEPT, PROP_ORG_ID, "D056890");
            assertThat(eukaryota.getProperty(PROP_PREF_NAME)).isEqualTo("Eukaryota");
            assertThat((String[])eukaryota.getProperty(PROP_SYNONYMS)).containsExactlyInAnyOrder("Eucarya", "Eukarya", "Eukaryotes");
            // We need to normalize the XML indentation here, thus the replaceAll
            assertThat((String[])eukaryota.getProperty(PROP_DESCRIPTIONS)).matches(desc -> desc[0].replaceAll("\\s+", " ").contains("They comprise almost all multicellular and many unicellular organisms"));

            tx.commit();
        }
    }

    @Test
    public void testSimpleXmlFormat() throws Exception {
        XMLConfiguration xmlConfiguration = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/simpleXmlImportConfig.xml"));
        HierarchicalConfiguration<ImmutableNode> connectionConfiguration = xmlConfiguration.configurationAt(CONNECTION);
        ConceptInsertionService insertionService = ConceptInsertionService.getInstance(connectionConfiguration);
        XmlConceptCreator xmlConceptCreator = new XmlConceptCreator();
        HierarchicalConfiguration<ImmutableNode> importConfig = xmlConfiguration.configurationAt(slash(IMPORTS, IMPORT));
        Stream<ImportConcepts> concepts = xmlConceptCreator.createConcepts(importConfig);
        for (ImportConcepts ic : concepts.collect(toCollection(ArrayList::new))) {
            insertionService.insertConcepts(importConfig, ic);
        }

        FileConnectionService fileConnectionService = FileConnectionService.getInstance();
        GraphDatabaseService graphdb = fileConnectionService.getDatabase(connectionConfiguration);
        try (Transaction tx = graphdb.beginTx()) {
            Node node = tx.findNode(CONCEPT, PROP_ORG_ID, "D007801");
            assertNotNull(node);
            // Check that the original source regular expression detection works
            assertThat(node.getProperty(PROP_ORG_SRC)).isEqualTo("MeSH XML");
            assertThat((String[])node.getProperty(PROP_SOURCES)).containsExactly("Immunology Concepts");

            tx.commit();
        }
    }

    @Test
    public void testMultipleConnectedImports() throws Exception {
        XMLConfiguration xmlConfiguration = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/multipleConnectedImportsConfig.xml"));
        HierarchicalConfiguration<ImmutableNode> connectionConfiguration = xmlConfiguration.configurationAt(CONNECTION);
        ConceptInsertionService insertionService = ConceptInsertionService.getInstance(connectionConfiguration);
        XmlConceptCreator xmlConceptCreator = new XmlConceptCreator();
        HierarchicalConfiguration<ImmutableNode> importConfig = xmlConfiguration.configurationAt(slash(IMPORTS, IMPORT));
        Stream<ImportConcepts> concepts = xmlConceptCreator.createConcepts(importConfig);
        for (ImportConcepts ic : concepts.collect(toCollection(ArrayList::new))) {
            insertionService.insertConcepts(importConfig, ic);
        }

        FileConnectionService fileConnectionService = FileConnectionService.getInstance();
        GraphDatabaseService graphdb = fileConnectionService.getDatabase(connectionConfiguration);
        // Tests for the Anatomy concepts in the test data
        try (Transaction tx = graphdb.beginTx()) {
            // The snippet contains - amongst others - the concept for "body parts" and some of its subclasses
            Node bodyRegions = tx.findNode(CONCEPT, PROP_ORG_ID, "D005063");
            assertNotNull(bodyRegions);
            Iterable<Relationship> relationships = bodyRegions.getRelationships(Direction.OUTGOING, ConceptEdgeTypes.IS_BROADER_THAN);
            // The descriptor for "head" should also be there
            assertThat(relationships).extracting(r -> r.getEndNode()).extracting(n -> n.getProperty(PROP_ORG_ID)).containsExactlyInAnyOrder("C114158");

            tx.commit();
        }
    }
}
