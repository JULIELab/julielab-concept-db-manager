package de.julielab.concepts.db.core;

import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.testng.annotations.Test;

import java.io.File;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.assertj.core.api.Assertions.assertThat;

public class ConfigTest {
    @Test
    public void testConfiguration() throws ConfigurationException {
        XMLConfiguration config = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/testconfig.xml"));
        config.setExpressionEngine(new XPathExpressionEngine());
        assertThat(config).extracting(
                c -> c.getString(slash(EXPORTS, EXPORT, CONFIGURATION, PLUGIN_ENDPOINT)),
                c -> c.getString(slash(EXPORTS, EXPORT, CONFIGURATION, PARAMETERS, "conceptlabel")),
                c -> c.getString(slash(EXPORTS, EXPORT, CONFIGURATION, PARAMETERS, "facetlabels", "facetlabel[1]")),
                c -> c.getString(slash(EXPORTS, EXPORT, CONFIGURATION, PARAMETERS, "facetlabels", "facetlabel[2]")))
                .contains("/db/data/ext/Export/graphdb/hypernyms", "ID_MAP_NCBI_GENES");
    }
}
