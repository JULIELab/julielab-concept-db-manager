package de.julielab.concepts.db.creators.mesh;

import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

/**
 * A helper class to check if a given ID is an original ID and if so, what it original source was. This is configured
 * in the XML configuration.
 */
public class ConceptSourceMatcher {
    private String orgRegex;
    private String orgSource;
    private String source;
    private String sourceFile;

    public ConceptSourceMatcher(HierarchicalConfiguration<ImmutableNode> inputConfig) {
        orgRegex = inputConfig.getString(MeshConceptCreator.ORG_ID_REGEX);
        orgSource = inputConfig.getString(MeshConceptCreator.ORG_SOURCE);

        try {
            source = ConfigurationUtilities.requirePresent(MeshConceptCreator.SOURCE_NAME, inputConfig::getString);
            sourceFile = ConfigurationUtilities.requirePresent(MeshConceptCreator.XMLFILE, inputConfig::getString);
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException(e);
        }
    }


    public String matchOriginalId(String originalId) {
        if (orgRegex != null && originalId.matches(orgRegex)) {
            return orgSource;
        } else if (orgRegex == null && orgSource != null) {
            return orgSource;
        }
        return null;
    }


    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSourceFile() {
        return sourceFile;
    }
}
