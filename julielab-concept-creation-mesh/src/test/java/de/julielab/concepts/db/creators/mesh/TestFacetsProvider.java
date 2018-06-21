package de.julielab.concepts.db.creators.mesh;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.exchange.DataImporter;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Class for testing <code>FacetsProvider</code>.
 * @author Philipp Lucas
 *
 */
public class TestFacetsProvider 
extends TestBase {

	private static Logger logger = org.slf4j.LoggerFactory
			.getLogger(TestFacetsProvider.class);
	
	@Test
	public void testFacetsProvider() throws Exception {    	
    	// import old intermediate MeSH XML data for semedico
    	Tree udMesh2008 = new Tree("UD-MeSH 2008");
    	DataImporter.fromUserDefinedMeshXml(udMeshPath, udMesh2008);
    	
    	// import MeSH 2008 data
    	Tree mesh2008 = new Tree("MeSH 2008");
    	DataImporter.fromOriginalMeshXml(meshPath + "desc2008.xml.gz", mesh2008);
    	
    	// determine modifications
    	// note: this is necessary to restore ud-mesh correctly. 
    	TreeComparatorUD comparator = new TreeComparatorUD(mesh2008,udMesh2008);
    	comparator.determineModifications();
    	
    	
    	// now we can test facets    	
    	FacetsProvider facets = new FacetsProvider(udMesh2008);
    	
    	assertNull("Facet of root vertex should be null", facets.getFacet(udMesh2008.getRootVertex()));
    	assertEquals("Facets of root descriptor should be empty set", 0, facets.getFacets(udMesh2008.getRootDesc()).size());
    	
    	for (TreeVertex v : udMesh2008.childVerticesOf( udMesh2008.getRootVertex() )) {
    		assertEquals("Facet of a facet vertex should be the name of that vertex", v.getName(), facets.getFacet(v));
    	}
    	    	
    	// some other randomly (manually) chosen vertices    	
    	String[] facetName = new String[] {"Facet Diseases / Pathological Processes"};   	
    	assertTrue(testDescriptorFacet(facetName, "D015423", udMesh2008, facets));
    	assertTrue(testDescriptorFacet(facetName, "D020431", udMesh2008, facets));
    	
    	facetName = new String[] {"Facet Chemicals and Drugs"};
    	assertTrue(testDescriptorFacet(facetName, "D000477", udMesh2008, facets));
    	assertTrue(testDescriptorFacet(facetName, "D000862", udMesh2008, facets));
    	
    	facetName = new String[] {"Facet Therapies and Treatments"};  
    	testDescriptorFacet(facetName, "D002405", udMesh2008, facets);
    	testDescriptorFacet(facetName, "D005065", udMesh2008, facets);
    	
    	facetName = new String[] {"Facet Signs and Symptoms", "Facet Diseases / Pathological Processes"};
    	assertTrue(testDescriptorFacet(facetName, "D000370", udMesh2008, facets));
    	assertTrue(testDescriptorFacet(facetName, "D001416", udMesh2008, facets));
    	
	}
	
	private boolean testDescriptorFacet (String[] facetNames, String dUi, Tree tree, FacetsProvider facets) {
		Descriptor desc = tree.getDescriptorByUi(dUi);

		logger.info("Facets of " + desc.toString() + " are = ");
    	for(String facet : facets.getFacets(desc)) {
    		logger.info(facet);
    	}
    	for (String facetName : facetNames) {
    		if (!facets.getFacets(desc).contains(facetName)) {
    			logger.warn("but \"{}\" was not returned as facet name", facetName);
    			return false;
    		}
    	}
////    	if (!facets.hasFacet(desc, facetNames)) {
//    		logger.warn("but should be: " + facetNames);
//    		return false;
//    	} else{
    	   	return true;   		
//    	}
	}

}
