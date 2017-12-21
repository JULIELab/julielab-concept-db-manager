package de.julielab.concepts.db.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.junit.Test;
import org.neo4j.shell.util.json.JSONArray;

import de.julielab.concepts.db.core.DataExporterBase.Parameter;

public class DataExporterBaseTest {
@SuppressWarnings("unchecked")
@Test
public void testParseParameters() throws Exception {
	Parameters params = new Parameters();
	FileBasedConfigurationBuilder<XMLConfiguration> builder =
	    new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
	    .configure(params.xml()
	        .setFileName("src/test/resources/testconfig.xml"));
	XMLConfiguration config = builder.getConfiguration();
	
	Method method = DataExporterBase.class.getDeclaredMethod("parseParameters", HierarchicalConfiguration.class);
	method.setAccessible(true);
	Map<String, Parameter> parsedParameters = (Map<String, Parameter>) method.invoke(new ServerPluginExporter(), config.configurationAt("configuration.parameters"));
	assertEquals(2, parsedParameters.size());
	List<Parameter> parametersList = new ArrayList<>(parsedParameters.values());
	assertNotNull(parametersList.get(0).getName());
	assertNotNull(parametersList.get(1).getName());
	
	Parameter conceptLabelParameter = parsedParameters.get("conceptlabel");
	assertNotNull(conceptLabelParameter);
	assertEquals("label", conceptLabelParameter.getName());
	assertEquals(String.class, conceptLabelParameter.getType());
	assertFalse(conceptLabelParameter.isList());
	assertEquals("ID_MAP_NCBI_GENES", conceptLabelParameter.getValue());
	
	Parameter facetLabelsParameter = parsedParameters.get("facetlabels");
	assertNotNull(facetLabelsParameter);
	assertEquals("labels", facetLabelsParameter.getName());
	assertTrue(facetLabelsParameter.isList());
	assertTrue(facetLabelsParameter.convertToJson());
	assertTrue(facetLabelsParameter.getRequestValue() instanceof String);
	JSONArray jsonArray = new JSONArray((String)facetLabelsParameter.getRequestValue());
	assertEquals(2, jsonArray.length());
	assertEquals("FACET", jsonArray.getString(0));
	assertEquals("BIO_PORTAL", jsonArray.getString(1));
}
}
