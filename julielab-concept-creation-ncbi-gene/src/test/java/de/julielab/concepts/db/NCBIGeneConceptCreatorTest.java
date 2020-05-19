package de.julielab.concepts.db;

import de.julielab.concepts.db.core.services.ConceptCreationService;
import de.julielab.concepts.db.core.services.ConceptInsertionService;
import de.julielab.concepts.db.core.services.FileConnectionService;
import de.julielab.concepts.db.creators.NCBIGeneConceptCreator;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.auxiliaries.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.concepts.ConceptEdgeTypes;
import de.julielab.neo4j.plugins.concepts.ConceptLabel;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities.*;
import static de.julielab.neo4j.plugins.concepts.ConceptLabel.AGGREGATE;
import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.*;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class NCBIGeneConceptCreatorTest {

    private static final File TEST_DB = new File("src/test/resources/graph.db");
    private static Logger log = LoggerFactory.getLogger(NCBIGeneConceptCreatorTest.class);

    @AfterMethod
    @BeforeMethod
    public void deleteDb() throws IOException {
        log.debug("Deleting the test DB directory");
        FileUtils.deleteDirectory(TEST_DB);
    }

    @Test
    public void testCreateGeneConcepts() throws Exception {
        Method method = NCBIGeneConceptCreator.class.getDeclaredMethod("createGeneTerm", String.class, Map.class);
        method.setAccessible(true);
        String geneRecord = "9606	3558	IL2	-	IL-2|TCGF|lymphokine	HGNC:6001|MIM:147680|HPRD:00979	4	4q26-q27	interleukin 2	protein-coding	IL2	interleukin 2	O	T cell growth factor|aldesleukin|interleukin-2|involved in regulation of T-cell clonal expansion	20140807";
        Map<String, String> gene2Summary = new HashMap<>();
        gene2Summary.put("3558", "This is a test summary.");
        NCBIGeneConceptCreator geneImporter = new NCBIGeneConceptCreator();
        ImportConcept term = (ImportConcept) method.invoke(geneImporter, geneRecord, gene2Summary);
        assertEquals("IL2", term.prefName);
        assertEquals("3558", term.coordinates.originalId);
        assertNotNull(term.descriptions);
        assertEquals(1, term.descriptions.size());
        assertEquals("This is a test summary.", term.descriptions.get(0));
        log.debug("Actual synonyms: {}", term.synonyms);
        // synonyms:
        // 1. official full name
        // 2. synonyms
        // 3. other designations
        assertArrayEquals(
                new String[]{"interleukin 2", "IL-2", "TCGF", "lymphokine", "T cell growth factor", "aldesleukin",
                        "interleukin-2", "involved in regulation of T-cell clonal expansion"},
                term.synonyms.toArray());
    }

    @Test
    public void testConvertGeneInfoToTerms() throws Exception {
        Method method = NCBIGeneConceptCreator.class.getDeclaredMethod("convertGeneInfoToTerms", File.class, File.class,
                File.class, Map.class, Map.class);
        method.setAccessible(true);
        NCBIGeneConceptCreator geneImporter = new NCBIGeneConceptCreator();
        Map<String, String> geneId2Tax = new HashMap<>();
        Map<String, ImportConcept> termsByGeneId = new HashMap<>();
        method.invoke(geneImporter, new File("src/test/resources/geneconcepts/gene_info_snippet"),
                new File("src/test/resources/geneconcepts/organisms_snippet.taxid"),
                new File("src/test/resources/geneconcepts/gene2Summary_snippet"), geneId2Tax, termsByGeneId);
        assertTrue(geneId2Tax.size() > 0);
        assertEquals("9606", geneId2Tax.get("3558"));
        // The keys are lowercased to avoid problems due to different casing.
        ImportConcept term = termsByGeneId.get(NCBIGeneConceptCreator.getGeneCoordinates("3558"));
        assertEquals("IL2", term.prefName);
        assertEquals("3558", term.coordinates.originalId);
        assertNotNull(term.descriptions);
        assertEquals(1, term.descriptions.size());
        assertEquals(
                "The protein encoded by this gene is a " + "secreted cytokine that is important for the "
                        + "proliferation of T and B lymphocytes. The receptor "
                        + "of this cytokine is a heterotrimeric protein complex "
                        + "whose gamma chain is also shared by interleukin 4 (IL4) "
                        + "and interleukin 7 (IL7). The expression of this gene "
                        + "in mature thymocytes is monoallelic, which represents "
                        + "an unusual regulatory mode for controlling the precise "
                        + "expression of a single gene. The targeted disruption of "
                        + "a similar gene in mice leads to ulcerative colitis-like "
                        + "disease, which suggests an essential role of this gene in "
                        + "the immune response to antigenic stimuli. [provided by RefSeq, Jul 2008]",
                term.descriptions.get(0));

        // the Acta1 rat gene is in our snippet of gene_info, however the rat was not
        // included in our organism file,
        // thus it should not exist in the map.
        assertNull(geneId2Tax.get("29437"));
        // mouse
        assertNotNull(geneId2Tax.get("11459"));
        // human
        assertNotNull(geneId2Tax.get("58"));
        // and chicken
        assertNotNull(geneId2Tax.get("421534"));
    }

    @Test
    public void importNcbiGeneConceptsTest() throws Exception {
        // This test performs an insertion a few genes excerpted from the full
        // resources.
        // Note that not even all genes in the given gene_info file are imported but
        // only
        // those belonging to taxonomy IDs given in the taxIdsForTests.lst file
        // referenced
        // in the configuration file. This restricts the final imported genes to a
        // rather
        // small set.

        XMLConfiguration configuration = ConfigurationUtilities
                .loadXmlConfiguration(new File("src/test/resources/geneconcepts/geneimport.xml"));
        ConceptCreationService conceptCreationService = ConceptCreationService.getInstance();
        HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
                .configurationAt(CONNECTION);
        ConceptInsertionService insertionService = ConceptInsertionService.getInstance(connectionConfiguration);


        HierarchicalConfiguration<ImmutableNode> importConfiguration = configuration.configurationAt(slash(IMPORTS, IMPORT));
        Stream<ImportConcepts> concepts = conceptCreationService.createConcepts(importConfiguration);
        insertionService.insertConcepts(importConfiguration, concepts);

        GraphDatabaseService graphdb = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
        Set<String> expectedGeneIds = new HashSet<>(Arrays.asList("11477", "172978", "24205"));
        try (Transaction tx = graphdb.beginTx()) {
            for (Node n : tx.getAllNodes()) {
                if (n.hasProperty(PROP_ORG_ID) && !n.hasLabel(AGGREGATE)) {
                    expectedGeneIds.remove(n.getProperty(PROP_ORG_ID));
                }
                // Check for the MTOR gene group that the orthologs have been collected as
                // intended. Remember that only a few tax IDs are imported, 9606 (human) is
                // missing for example. Thus, the human MTOR gene with ID 2475 is not imported
                // itself but the gene_group node is identified by this ID nontheless.
                if (n.hasProperty(PROP_ORG_ID) && n.getProperty(PROP_ORG_ID).equals("2475") && n.hasLabel(AGGREGATE)) {
                    Set<String> expectedOrthoGenes = new HashSet<>(Arrays.asList("56718", "56717"));
                    for (Relationship rel : n.getRelationships(Direction.OUTGOING, ConceptEdgeTypes.HAS_ELEMENT)) {
                        assertTrue(expectedOrthoGenes.remove(rel.getEndNode().getProperty(PROP_ORG_ID)));
                    }
                    assertTrue("The following gene IDs where not found in the MTOR orthologs aggregate: "
                            + expectedOrthoGenes, expectedOrthoGenes.isEmpty());
                }
            }
            tx.commit();
        }
        assertTrue("The following gene IDs where not found in the database: " + expectedGeneIds,
                expectedGeneIds.isEmpty());
        FileConnectionService.getInstance().shutdown();
    }

    @Test
    public void testMultipleAggregation() throws Exception {
        // The test data of this test requires an aggregate for homologs, multiple aggregates for orthologs, an ortholog aggregate and a top homology aggregate.
        // View the data structure by consulting the image in the src/test/resources/geneconcepts/syntheticimports/orthologs/datastructure.mathml/pdf file
        // to better understand the node names and the tested structure
        final XMLConfiguration configuration = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/geneconcepts/syntheticimports/orthologs/geneimport.xml"));
        ConceptCreationService conceptCreationService = ConceptCreationService.getInstance();
        HierarchicalConfiguration<ImmutableNode> connectionConfiguration = configuration
                .configurationAt(CONNECTION);
        ConceptInsertionService insertionService = ConceptInsertionService.getInstance(connectionConfiguration);


        HierarchicalConfiguration<ImmutableNode> importConfiguration = configuration.configurationAt(slash(IMPORTS, IMPORT));
        Stream<ImportConcepts> concepts = conceptCreationService.createConcepts(importConfiguration);
        insertionService.insertConcepts(importConfiguration, concepts);

        GraphDatabaseService graphdb = FileConnectionService.getInstance().getDatabase(connectionConfiguration);
        try (Transaction tx = graphdb.beginTx()) {
            final List<Node> geneConcepts = tx.findNodes(ConceptLabel.CONCEPT).stream().filter(c -> !c.hasLabel(ConceptLabel.AGGREGATE) && c.hasProperty(ConceptConstants.PROP_ORG_ID) && ((String) c.getProperty(PROP_ORG_ID)).matches("g[0-9][0-9]?")).collect(Collectors.toList());
            for (Node n : geneConcepts) {
                assertThat(getParentNodes(n)).isNotEmpty();
                final Set<Node> geneOrthologAggregates = getParentNodes(n).stream().filter(p -> p.hasLabel(Label.label("AGGREGATE_GENEGROUP"))).collect(Collectors.toSet());
                for (Node orthologAggregate : geneOrthologAggregates) {
                    final Set<String> elementNames = getElementNodes(orthologAggregate).stream().map(e -> e.getProperty(PROP_PREF_NAME)).map(String.class::cast).collect(Collectors.toSet());
                    switch ((String) orthologAggregate.getProperty(PROP_ORG_ID)) {
                        case "g1":
                            assertThat(elementNames).containsExactlyInAnyOrder("name1", "name2");
                            break;
                        case "g2":
                            assertThat(elementNames).containsExactlyInAnyOrder("name2", "name3", "name4");
                            break;
                        case "g4":
                            assertThat(elementNames).containsExactlyInAnyOrder("name4", "name5", "name6", "name7");
                            break;
                        case "g7":
                            assertThat(elementNames).containsExactlyInAnyOrder("name7", "name8", "name9");
                            break;
                        case "g11":
                            assertThat(elementNames).containsExactlyInAnyOrder("name10", "name11");
                            break;
                        default:
                            throw new IllegalStateException("Unexpected orthology aggregate node: " + NodeUtilities.getNodePropertiesAsString(orthologAggregate));
                    }
                    final Set<Node> aggregatingNodes = getAggregatingNodes(orthologAggregate);
                    // Each orthology cluster should be aggregated by a single top orthology node except the cluster g11 which only connects the gene nodes g10 and g11 and does not take part in a homology cluster
                    if (!orthologAggregate.getProperty(PROP_ORG_ID).equals("g11")) {
                        assertThat(aggregatingNodes.size()).isEqualTo(1);
                        final Node aggregateOfOrthologAggregate = aggregatingNodes.stream().findAny().get();
                        final String aggregateSourceId = ((String[]) aggregateOfOrthologAggregate.getProperty(PROP_SRC_IDS))[0];
                        assertThat(aggregateSourceId).overridingErrorMessage("Expecting the node with properties <%s> to be element of the aggregate <%s> but was <%s> instead", PropertyUtilities.getNodePropertiesAsString(orthologAggregate), NCBIGeneConceptCreator.TOP_ORTHOLOGY_PREFIX + 0, aggregateSourceId).isEqualTo(NCBIGeneConceptCreator.TOP_ORTHOLOGY_PREFIX + 0);
                    } else {
                        // this is orthology cluster g11, there shouldn't be any governing aggregate
                        assertThat(aggregatingNodes).isEmpty();
                    }
                }
            }


            // There should be 5 gene group/ortholog clusters
            final List<Node> topOrthologyAggregates = tx.findNodes(Label.label("AGGREGATE_GENEGROUP")).stream().collect(Collectors.toList());
            assertThat(topOrthologyAggregates.size()).isEqualTo(5);


            tx.commit();
        }
        FileConnectionService.getInstance().shutdown();
    }


}
