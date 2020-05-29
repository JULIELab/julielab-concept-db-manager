package de.julielab.concepts.db.creators.mesh;

import de.julielab.concepts.db.core.spi.FacetCreator;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetGroup;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.List;

import static de.julielab.concepts.db.core.ConfigurationConstants.CREATOR;
import static de.julielab.concepts.db.core.ConfigurationConstants.REQUEST;
import static de.julielab.concepts.db.core.DefaultFacetCreator.FACET_GROUP_NAME;
import static de.julielab.concepts.db.core.DefaultFacetCreator.LABELS;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class SimpleXmlFacetCreator implements FacetCreator {
    @Override
    public ImportFacet createFacet(HierarchicalConfiguration<ImmutableNode> facetConfiguration, Object facetData) throws FacetCreationException {
        String configPath = slash(CREATOR, REQUEST);
        String facetGroupName = facetConfiguration.getString(slash(configPath, FACET_GROUP_NAME), "Default Facet Group");
        List<String> labels = facetConfiguration.getList(String.class, slash(configPath, LABELS));

        ImportFacetGroup fg = new ImportFacetGroup(facetGroupName);
        return new ImportFacet(fg, null, (String)facetData, "", FacetConstants.SRC_TYPE_HIERARCHICAL, labels, false);
    }



    @Override
    public String getName() {
        return getClass().getSimpleName();
    }
}
