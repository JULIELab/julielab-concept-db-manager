package de.julielab.concepts.db.creators.mesh;

import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import static de.julielab.java.utilities.ConfigurationUtilities.slash;

/**
 * A helper class to check if a given ID is an original ID and if so, what it original source was. This is configured
 * in the XML configuration.
 */
public class ConceptSourceMatcher {
    private String orgRegex;
    private String orgSource;

    public ConceptSourceMatcher(HierarchicalConfiguration<ImmutableNode> inputConfig) {
        try {
            orgSource = ConfigurationUtilities.requirePresent("", inputConfig::getString);
            orgRegex = inputConfig.getString("@" + XmlConceptCreator.REGEX);
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

}
