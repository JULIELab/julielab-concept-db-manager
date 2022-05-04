package de.julielab.concepts.db.creators;

import de.julielab.concepts.db.core.DefaultFacetCreator;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.concepts.db.core.DefaultFacetCreator.SOURCE_TYPE;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.assertj.core.api.Assertions.assertThat;
class HgncGroupsConceptCreatorTest {

    @Test
    void createConcepts() throws Exception{
        HgncGroupsConceptCreator creator = new HgncGroupsConceptCreator();
        HierarchicalConfiguration<ImmutableNode> configuration = ConfigurationUtilities.createEmptyConfiguration();
        String resourcesDir = Path.of("src", "test", "resources").toString();
        // concept creation settings
        String confPath = slash(CONCEPTS, CREATOR, CONFIGURATION);
        configuration.setProperty(slash(confPath, HgncGroupsConceptCreator.FAMILYFILE), Path.of(resourcesDir, "family_small.csv").toString());
        configuration.setProperty(slash(confPath, HgncGroupsConceptCreator.FAMILYALIASFILE), Path.of(resourcesDir, "family_alias_small.csv").toString());
        configuration.setProperty(slash(confPath, HgncGroupsConceptCreator.HIERARCHYFILE), Path.of(resourcesDir, "hierarchy_small.csv").toString());
        configuration.setProperty(slash(confPath, HgncGroupsConceptCreator.GENETOGROUPMAP), Path.of(resourcesDir, "gene_group_map_small.tsv").toString());
        // facet creation settings
        String facetConfPath = slash(FACET, CREATOR, CONFIGURATION);
        configuration.addProperty(slash(FACET, CREATOR, NAME), DefaultFacetCreator.class.getSimpleName());
        configuration.setProperty(slash(facetConfPath, FACET_GROUP, NAME), "Biology");
        configuration.setProperty(slash(facetConfPath,  NAME), "Gene Groups");
        configuration.setProperty(slash(facetConfPath, FACET_GROUP, SOURCE_TYPE), FacetConstants.SRC_TYPE_HIERARCHICAL);


        Optional<ImportConcepts> concepts = creator.createConcepts(configuration).findAny();
        assertThat(concepts).isPresent();
        assertThat(concepts.get().getNumConcepts() > 0);
        final Map<String, ImportConcept> id2concept = concepts.get().getConcepts().collect(Collectors.toMap(c -> c.coordinates.originalId, Function.identity()));
        // check that the group concepts are present
        assertThat(id2concept).containsKey("161");
        assertThat(id2concept).containsKey("170");
        assertThat(id2concept).containsKey("172");
        assertThat(id2concept).containsKey("177");
        assertThat(id2concept).containsKey("294");

        // check the names of the groups
        assertThat(id2concept).extractingByKey("161").extracting("prefName").isEqualTo("Ligand gated ion channels");
        assertThat(id2concept).extractingByKey("170").extracting("prefName").isEqualTo("HTR");
        assertThat(id2concept).extractingByKey("172").extracting("prefName").isEqualTo("HTR3");
        assertThat(id2concept).extractingByKey("177").extracting("prefName").isEqualTo("Ion channels");
        assertThat(id2concept).extractingByKey("294").extracting("prefName").isEqualTo("Ion channels by gating mechanism");

        // check hierarchy
        assertThat(id2concept.get("161").parentCoordinates).extracting("sourceId").contains("294");
        assertThat(id2concept.get("172").parentCoordinates).extracting("sourceId").contains("161");
        assertThat(id2concept.get("294").parentCoordinates).extracting("sourceId").contains("177");

        // check genes that are part of the groups

        assertThat(id2concept.get("3359").parentCoordinates).extracting("sourceId").contains("172");
        assertThat(id2concept.get("3350").parentCoordinates).extracting("sourceId").contains("170");
        assertThat(id2concept.get("3355").parentCoordinates).extracting("sourceId").contains("170");
    }
}