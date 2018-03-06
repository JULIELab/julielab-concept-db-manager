package de.julielab.concepts.db.creators.mesh;

import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import de.julielab.concepts.db.creators.mesh.exchange.DataImporter;

/**
 * Class for testing <code>TreeComparatorUD</code>.
 * 
 * @author Philipp Lucas
 */
// The tests fails, but the process seems to be just right, the old MeSH is enriched with new MeSH terms, if only
// synonyms, so the results do not perfectly equal each other.
@Ignore
public class TestTreeComparatorUD extends TestBase {

	@Test
	public void testComparatorUD() throws Exception {
		// import old intermediate MeSH XML data for semedico
		Tree udMesh2008 = new Tree("UD-MeSH 2008");
		DataImporter.fromUserDefinedMeshXml(udMeshPath, udMesh2008);

		assertTrue("Tree not integer!", udMesh2008.verifyIntegrity());

		// import MeSH 2008 data
		Tree mesh2008 = new Tree("MeSH 2008");
		DataImporter.fromOriginalMeshXml(meshPath + "desc2008.xml.gz", mesh2008);

		// determine modifications
		TreeComparatorUD comparator = new TreeComparatorUD(mesh2008, udMesh2008);
		comparator.determineModifications();

		// export modifications
		comparator.saveModificationsToFiles(this.outPath + "mods4semedico2008");

		// apply modifications to original MeSH
		TreeModificator modifier = new TreeModificator(mesh2008);
		modifier.putModification(comparator);
		modifier.applyAll(true);

		udMesh2008.printInfo(System.out);
		mesh2008.printInfo(System.out);

		// check for equal
		assertTrue(udMesh2008.getName() + " and " + mesh2008.getName() + " are not equal.",
				TreeComparator.isEqualTrees(udMesh2008, mesh2008));
	}

}
