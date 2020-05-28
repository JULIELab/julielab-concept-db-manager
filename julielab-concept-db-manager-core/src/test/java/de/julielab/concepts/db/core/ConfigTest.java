package de.julielab.concepts.db.core;

import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.assertj.core.api.Assertions.assertThat;

public class ConfigTest {
    @Test
    public void testConfiguration() throws ConfigurationException {
        XMLConfiguration config = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/functioncalltestconfig.xml"));
        config.setExpressionEngine(new XPathExpressionEngine());
        assertThat(config).extracting(
                c -> c.getString(slash(EXPORTS, EXPORT, CONFIGURATION, REST_ENDPOINT)),
                c -> c.getString(slash(EXPORTS, EXPORT, CONFIGURATION, PARAMETERS, "conceptlabel")),
                c -> c.getString(slash(EXPORTS, EXPORT, CONFIGURATION, PARAMETERS, "facetlabels", "facetlabel[1]")),
                c -> c.getString(slash(EXPORTS, EXPORT, CONFIGURATION, PARAMETERS, "facetlabels", "facetlabel[2]")))
                .contains("/db/data/ext/Export/graphdb/hypernyms", "ID_MAP_NCBI_GENES");
    }

    @Test
    public void muh() throws URISyntaxException {
        java.net.URI uri = new java.net.URI("http", "localhost:9200", "/concepts/export", "param1=value1&param2value2", "thefrag");
        java.net.URI uri2 = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), "param1=value1&param2value2", null);
        java.net.URI uri3 = java.net.URI.create("http://localhost:33300/concepts/concept_aggregate_manager/build_aggregates_by_mappings");
        java.net.URI uri4 = new URI("http",null, "localhost", 9200, "/concepts", null, null);
        System.out.println(uri);
        System.out.println(uri2);
        System.out.println(uri2.getAuthority());
        System.out.println(uri3);
        System.out.println(uri4);
    }
}
