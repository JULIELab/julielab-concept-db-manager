package de.julielab.concepts.db.creators.mesh;

import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class ConceptSourceMatcher {
    private String orgRegex;
    private String orgSource;
    private String regex;
    private String source;
    private String sourceFile;

    private String noMatchOrgSource;
    private String noMatchSource;

    public ConceptSourceMatcher(HierarchicalConfiguration<ImmutableNode> inputConfig) {
        orgRegex = inputConfig.getString(MeshConceptCreator.ORG_ID_REGEX);
        orgSource = inputConfig.getString(MeshConceptCreator.ORG_SOURCE);
        regex = inputConfig.getString(MeshConceptCreator.SRC_ID_REGEX);
        source = inputConfig.getString(MeshConceptCreator.SOURCE);

        noMatchOrgSource = inputConfig.getString(MeshConceptCreator.NO_MATCH_ORG_SOURCE);
        noMatchSource = inputConfig.getString(MeshConceptCreator.NO_MATCH_SOURCE);

        sourceFile = inputConfig.getString(MeshConceptCreator.XMLFILE);
    }

    public String matchSourceId(String sourceId) {
        if (regex != null && sourceId.matches(regex)) {
            return source;
        } else if (regex == null && source != null) {
            return source;
        } else if (noMatchSource != null) {
            return noMatchSource;
        }
        return null;
    }

    public String matchOriginalId(String originalId) {
        if (orgRegex != null && originalId.matches(orgRegex)) {
            return orgSource;
        } else if (orgRegex == null && orgSource != null) {
            return orgSource;
        } else if (noMatchOrgSource != null) {
            return noMatchOrgSource;
        }
        return null;
    }


    public void setNoMatchOrgSource(String noMatchOrgSource) {
        this.noMatchOrgSource = noMatchOrgSource;
    }


    public void setNoMatchSource(String noMatchSource) {
        this.noMatchSource = noMatchSource;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getOrgRegex() {
        return orgRegex;
    }

    public void setOrgRegex(String orgRegex) {
        this.orgRegex = orgRegex;
    }

    public String getOrgSource() {
        return orgSource;
    }

    public void setOrgSource(String orgSource) {
        this.orgSource = orgSource;
    }

    public String getSourceFile() {
        return sourceFile;
    }
}
