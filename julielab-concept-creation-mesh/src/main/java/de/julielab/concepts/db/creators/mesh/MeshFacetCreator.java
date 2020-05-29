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

public class MeshFacetCreator implements FacetCreator {
    @Override
    public ImportFacet createFacet(HierarchicalConfiguration<ImmutableNode> facetConfiguration, Object facetData) throws FacetCreationException {
        String configPath = slash(CREATOR, REQUEST);
        String facetGroupName = facetConfiguration.getString(slash(configPath, FACET_GROUP_NAME), "Default Facet Group");
        List<String> labels = facetConfiguration.getList(String.class, slash(configPath, LABELS));

        String effectiveName = getFacetName((String) facetData);

        ImportFacetGroup fg = new ImportFacetGroup(facetGroupName);
        return new ImportFacet(fg, null, effectiveName, "", FacetConstants.SRC_TYPE_HIERARCHICAL, labels, false);
    }

    private String getFacetName(String meshTreeIdentifier) throws FacetCreationException {
        String facetName;
        switch (meshTreeIdentifier) {
            case "A":
                facetName = "Anatomy";
                break;
            case "B":
                facetName = "Organisms";
                break;
            case "C":
                facetName = "Diseases";
                break;
            case "D":
                facetName = "Chemicals and Drugs";
                break;
            case "E":
                facetName = "Analytical, Diagnostic and Therapeutic Techniques and Equipment";
                break;
            case "F":
                facetName = "Psychiatry and Psychology";
                break;
            case "G":
                facetName = "Phenomena and Processes";
                break;
            case "H":
                facetName = "Disciplines and Occupations";
                break;
            case "I":
                facetName = "Anthropology, Education, Sociology and Social Phenomena";
                break;
            case "J":
                facetName = "Technology, Industry, Agriculture";
                break;
            case "K":
                facetName = "Humanities";
                break;
            case "L":
                facetName = "Information Science";
                break;
            case "M":
                facetName = "Named Groups";
                break;
            case "N":
                facetName = "Health Care";
                break;
            case "V":
                facetName = "Publication Characteristics";
                break;
            case "Z":
                facetName = "Geographicals";
                break;
            default:
                throw new FacetCreationException("Facet with name \"" + meshTreeIdentifier + "\" is unknown.");
        }
        return facetName;
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }
}
