package de.julielab.concepts.db.creators;

import de.julielab.concepts.db.core.DefaultFacetCreator;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConceptRelationship;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.concepts.db.core.DefaultFacetCreator.SOURCE_TYPE;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.assertj.core.api.Assertions.assertThat;
class FamPlexConceptCreatorTest {

    @Test
    void createConcepts() throws Exception {
        FamPlexConceptCreator creator = new FamPlexConceptCreator();
        HierarchicalConfiguration<ImmutableNode> configuration = ConfigurationUtilities.createEmptyConfiguration();
        String resourcesDir = Path.of("src", "test", "resources").toString();
        // concept creation settings
        String confPath = slash(CONCEPTS, CREATOR, CONFIGURATION);
        configuration.setProperty(slash(confPath, FamPlexConceptCreator.RELATIONSFILE), Path.of(resourcesDir, "relations_egids_small.tsv").toString());
        configuration.setProperty(slash(confPath, FamPlexConceptCreator.GROUNDINGMAP), Path.of(resourcesDir, "grounding_map_small.tsv").toString());
        configuration.setProperty(slash(confPath, FamPlexConceptCreator.NAMEEXTENSIONRECORDS), Path.of(resourcesDir, "expanded_small.dict").toString());
        // facet creation settings
        String facetConfPath = slash(FACET, CREATOR, REQUEST);
        configuration.addProperty(slash(FACET, CREATOR, NAME), DefaultFacetCreator.class.getSimpleName());
        configuration.setProperty(slash(facetConfPath, FACET_GROUP, NAME), "Biology");
        configuration.setProperty(slash(facetConfPath,  NAME), "Protein Complexes");
        configuration.setProperty(slash(facetConfPath, FACET_GROUP, SOURCE_TYPE), FacetConstants.SRC_TYPE_HIERARCHICAL);


        Optional<ImportConcepts> concepts = creator.createConcepts(configuration).findAny();
        assertThat(concepts).isPresent();

        // Check that the facet has been created as expected.
        assertThat(concepts.get().getFacet().getFacetGroup().name).isEqualTo("Biology");
        assertThat(concepts.get().getFacet().getName()).isEqualTo("Protein Complexes");
        assertThat(concepts.get().getFacet().getSourceType()).isEqualTo(FacetConstants.SRC_TYPE_HIERARCHICAL);

        List<ImportConcept> conceptList = concepts.get().getConcepts().collect(Collectors.toList());
        // Concepts must specify a source and source ID
        assertThat(conceptList).extracting("coordinates").extracting("source").doesNotContainNull();
        assertThat(conceptList).extracting("coordinates").extracting("sourceId").doesNotContainNull();

        // Checks for FamPlex Concepts ------------------
        List<ImportConcept> famplexConcepts = conceptList.stream().filter(c -> c.coordinates.originalSource.equals("FPLX")).collect(Collectors.toList());
        List<String> famplexIds = famplexConcepts.stream().map(c -> c.coordinates.originalId).collect(Collectors.toList());
        // Check that all FamPlex concepts mentioned in either relations or the grounding map have been created
        assertThat(famplexIds).contains("Activin_A", "Activin_AB", "Activin", "ACAD", "AKT", "AMPK");

        // Check that the concepts have all received their label
        for (ImportConcept famplexConcept : famplexConcepts)
            assertThat(famplexConcept.generalLabels).contains(FamPlexConceptCreator.LABEL_FAMPLEX);

        // Check that the AMPK concept has received its additional names from the expanded dictionary
        Optional<ImportConcept> ampkOpt = conceptList.stream().filter(c -> c.coordinates.originalId.equals("AMPK")).findAny();
        assertThat(ampkOpt).isPresent();
        // The abbreviations and acronyms of the expanded dictionary should have gone into the synonyms field
        assertThat(ampkOpt.get().synonyms).contains("AMP activated kinase", "adenosine monophosphate activated kinase", "adenosine monophosphate-activated protein kinase", "AMP activated protein kinase");
        // The spellings and inflections should have gone into the writingVariants field
        assertThat(ampkOpt.get().writingVariants).contains("AMP activated kinase", "AMP activated kinases", "AMP activated protein kinase", "AMP activated protein kinases", "AMP-activated kinase", "AMP-activated kinases", "AMP-activated protein kinase", "AMP-activated protein kinases", "AMPK", "AMPK's", "AMPKs", "adenosine monophosphate activated kinase", "adenosine monophosphate activated kinases", "adenosine monophosphate-activated kinase", "adenosine monophosphate-activated kinases", "adenosine monophosphate-activated protein kinase", "adenosine monophosphate-activated protein kinases", "AMP activated kinase", "AMP activated protein kinase", "AMP-activated kinase", "AMP-activated protein kinase", "adenosine monophosphate activated kinase", "adenosine monophosphate-activated kinase", "adenosine monophosphate-activated protein kinase");

        // Check concept relationsship:
        // FPLX	Activin_A	isa	FPLX	Activin
        // there should be the isa as well as the parent relationship for Activin_A
        Optional<ImportConcept> activinAOpt = famplexConcepts.stream().filter(c -> c.coordinates.originalId.equals("Activin_A")).findAny();
        assertThat(activinAOpt).isPresent();
        checkRelationShip(activinAOpt.get(), "isa", "FPLX", "Activin");

        // And here we should find a "partof" relationship:
        // FPLX	AMPK_alpha	partof	FPLX	AMPK
        Optional<ImportConcept> ampkAlphaOpt = famplexConcepts.stream().filter(c -> c.coordinates.originalId.equals("AMPK_alpha")).findAny();
        assertThat(ampkAlphaOpt).isPresent();
        checkRelationShip(ampkAlphaOpt.get(), "partof", "FPLX", "AMPK");

        // Checks for Non-FamPlex Concepts ------------------
        List<ImportConcept> nonFamplexConcepts = conceptList.stream().filter(c -> !c.coordinates.originalSource.equals("FPLX")).collect(Collectors.toList());
        // There should only be EntrezGene IDs besides the FamPlex IDs
        Set<String> nonFamplexSource = nonFamplexConcepts.stream().map(c -> c.coordinates.originalSource).collect(Collectors.toSet());
        assertThat(nonFamplexSource).containsExactly("NCBI Gene");
        List<String> nonFamplexIds = nonFamplexConcepts.stream().map(c -> c.coordinates.originalId).collect(Collectors.toList());
        assertThat(nonFamplexIds).contains("3623", "3624", "3625", "207", "208", "10000");

        // Check an isa relation:
        // EG	207	isa	FPLX	AKT
        Optional<ImportConcept> egAktOpt = nonFamplexConcepts.stream().filter(c -> c.coordinates.originalId.equals("207")).findAny();
        assertThat(egAktOpt).isPresent();
        checkRelationShip(egAktOpt.get(), "isa", "FPLX", "AKT");

        // Check a partof relation:
        // EG	3625	partof	FPLX	Inhibin_B
        Optional<ImportConcept> egInhibinOpt = nonFamplexConcepts.stream().filter(c -> c.coordinates.originalId.equals("3625")).findAny();
        assertThat(egInhibinOpt).isPresent();
        checkRelationShip(egInhibinOpt.get(), "partof", "FPLX", "Inhibin_B");
    }

    private void checkRelationShip(ImportConcept concept, String relationshipType, String targetSource, String targetId) {
        Optional<ImportConceptRelationship> relationshipOpt = concept.relationships.stream().filter(r -> r.targetCoordinates.originalId.equals(targetId) && r.targetCoordinates.originalSource.equals(targetSource) && r.type.equals(relationshipType)).findAny();
        assertThat(relationshipOpt).isPresent();
        Optional<ConceptCoordinates> parentOpt = concept.parentCoordinates.stream().filter(c -> c.originalSource.equals(targetSource) && c.originalId.equals(targetId)).findAny();
        assertThat(parentOpt).isPresent();
    }
}