package de.julielab.concepts.db.core;

import de.julielab.concepts.db.core.FunctionCallBase.Parameter;
import de.julielab.java.utilities.ConfigurationUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.json.JSONArray;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

public class FunctionCallBaseTest {
    @SuppressWarnings("unchecked")
    @Test
    public void testParseParameters() throws Exception {
        // This tests the case where the parameter elements are already named after the parameter
        // (in constrast to using the parametername attribute).
        XMLConfiguration config = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/functioncalltestconfig.xml"));

        Method method = FunctionCallBase.class.getDeclaredMethod("parseParameters", HierarchicalConfiguration.class);
        method.setAccessible(true);
        Map<String, Parameter> parsedParameters = (Map<String, Parameter>) method.invoke(new ServerPluginExporter(), config.configurationAt(slash(EXPORTS, EXPORT, CONFIGURATION, PARAMETERS)));
        assertEquals(2, parsedParameters.size());
        List<Parameter> parametersList = new ArrayList<>(parsedParameters.values());
        assertNotNull(parametersList.get(0).getName());
        assertNotNull(parametersList.get(1).getName());
        Parameter conceptLabelParameter = parsedParameters.get("label");
        assertNotNull(conceptLabelParameter);
        assertEquals("label", conceptLabelParameter.getName());
        assertEquals(String.class, conceptLabelParameter.getType());
        assertFalse(conceptLabelParameter.isList());
        assertEquals("ID_MAP_NCBI_GENES", conceptLabelParameter.getValue());

        Parameter facetLabelsParameter = parsedParameters.get("labels");
        assertNotNull(facetLabelsParameter);
        assertEquals("labels", facetLabelsParameter.getName());
        assertTrue(facetLabelsParameter.isList());
        assertTrue(facetLabelsParameter.convertToJson());
        assertTrue(facetLabelsParameter.getRequestValue() instanceof String);
        JSONArray jsonArray = new JSONArray((String) facetLabelsParameter.getRequestValue());
        assertEquals(2, jsonArray.length());
        assertEquals("FACET", jsonArray.getString(0));
        assertEquals("BIO_PORTAL", jsonArray.getString(1));
    }

    @Test
    public void testParseParameterNamesAsAttributes() throws Exception {
        // Basically the same test as above except there in the configuration used here the parameter elements
        // are all called "parameter" and specify their actual parameter name via the parametername attribute.
        XMLConfiguration config = ConfigurationUtilities.loadXmlConfiguration(new File("src/test/resources/functioncalltestconfigparameternamesasattributes.xml"));

        Method method = FunctionCallBase.class.getDeclaredMethod("parseParameters", HierarchicalConfiguration.class);
        method.setAccessible(true);
        Map<String, Parameter> parsedParameters = (Map<String, Parameter>) method.invoke(new ServerPluginExporter(), config.configurationAt(slash(EXPORTS, EXPORT, CONFIGURATION, PARAMETERS)));
        assertEquals(2, parsedParameters.size());
        List<Parameter> parametersList = new ArrayList<>(parsedParameters.values());
        assertNotNull(parametersList.get(0).getName());
        assertNotNull(parametersList.get(1).getName());
        Parameter conceptLabelParameter = parsedParameters.get("conceptlabel");
        assertNotNull(conceptLabelParameter);
        assertEquals("conceptlabel", conceptLabelParameter.getName());
        assertEquals(String.class, conceptLabelParameter.getType());
        assertFalse(conceptLabelParameter.isList());
        assertEquals("ID_MAP_NCBI_GENES", conceptLabelParameter.getValue());

        Parameter facetLabelsParameter = parsedParameters.get("facetlabels");
        assertNotNull(facetLabelsParameter);
        assertEquals("facetlabels", facetLabelsParameter.getName());
        assertTrue(facetLabelsParameter.isList());
        assertTrue(facetLabelsParameter.convertToJson());
        assertTrue(facetLabelsParameter.getRequestValue() instanceof String);
        JSONArray jsonArray = new JSONArray((String) facetLabelsParameter.getRequestValue());
        assertEquals(2, jsonArray.length());
        assertEquals("FACET", jsonArray.getString(0));
        assertEquals("BIO_PORTAL", jsonArray.getString(1));
    }
}
