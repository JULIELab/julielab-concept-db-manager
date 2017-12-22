package de.julielab.concepts.db;

import static de.julielab.concepts.db.core.RootConfigurationConstants.CONFKEY_CONNECTION;
import static de.julielab.concepts.db.core.RootConfigurationConstants.CONFKEY_IMPORT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.core.services.ConceptCreationService;
import de.julielab.concepts.db.core.services.ConceptInsertionService;
import de.julielab.concepts.db.creators.NCBIGeneConceptCreator;
import de.julielab.concepts.util.ConfigurationHelper;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.TermCoordinates;

public class NCBIGeneConceptCreatorTest {

	private static Logger log = LoggerFactory.getLogger(NCBIGeneConceptCreatorTest.class);

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
				new String[] { "interleukin 2", "IL-2", "TCGF", "lymphokine", "T cell growth factor", "aldesleukin",
						"interleukin-2", "involved in regulation of T-cell clonal expansion" },
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
		ImportConcept term = termsByGeneId.get(new TermCoordinates("3558", NCBIGeneConceptCreator.NCBI_GENE_SOURCE));
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
		XMLConfiguration configuration = ConfigurationHelper
				.loadXmlConfiguration(new File("src/test/resources/geneconcepts/geneimport.xml"));
		ConceptCreationService conceptCreationService = ConceptCreationService.getInstance();
		ConceptInsertionService insertionService = ConceptInsertionService
				.getInstance(configuration.configurationAt(CONFKEY_CONNECTION));

		HierarchicalConfiguration<ImmutableNode> importConfiguration = configuration.configurationAt(CONFKEY_IMPORT);
		Stream<ImportConcepts> concepts = conceptCreationService.createConcepts(importConfiguration);
		insertionService.insertConcepts(importConfiguration, concepts);

	}
}
