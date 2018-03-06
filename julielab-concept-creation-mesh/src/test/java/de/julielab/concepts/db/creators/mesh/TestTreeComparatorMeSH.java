package de.julielab.concepts.db.creators.mesh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import de.julielab.concepts.db.creators.mesh.exchange.DataExporter;
import de.julielab.concepts.db.creators.mesh.exchange.DataImporter;
import de.julielab.concepts.db.creators.mesh.modifications.DescAdditions;
import de.julielab.concepts.db.creators.mesh.modifications.DescDeletions;
import de.julielab.concepts.db.creators.mesh.modifications.DescRenamings;
import de.julielab.concepts.db.creators.mesh.modifications.VertexAdditions;
import de.julielab.concepts.db.creators.mesh.modifications.VertexDeletions;
import de.julielab.concepts.db.creators.mesh.modifications.VertexMovings;

/**
 * Class for testing <code>TreeComparatorMeSH</code.
 * @author Philipp Lucas
 *
 */
public class TestTreeComparatorMeSH 
extends TestBase {
	
	@Test
	public void testDetermineRenamingsAndRebindings() {
		descriptorRenamingsAndRebindings();
	}

	@Test
	public void testDetermineAddsAndMovings() {
		easyMoving();
		deepMoving();
		vertexAddition();
		descriptorAddition();
	}
	
	@Test
	public void testDetermineDeletions() {
		vertexDeletion_easy();
		vertexDeletion_complex();
	}
	
	@Test
	public void testComplex() {
		testComplexAll();
	}
	
	/**
	 * Tests the following:
	 * <ol>
	 * 	<li> import a source and a target </li> 
	 *  <li> determine modifications </li>
	 *  <li> applies modifications to source </li>
	 *  <li> check if both are equal </li>
	 * </ol>
	 */
	@Test
	public void testComplexApply() {

		Tree source = new Tree("complex_source.xml");
		DescAdditions descs = DataImporter.fromOwnXML(testInPath + "complex_source.xml");
		descs.apply(source);
		source.verifyIntegrity();

		Tree target = new Tree("complex_target.xml");
		descs = DataImporter.fromOwnXML(testInPath + "complex_target.xml");
		descs.apply(target);
		target.verifyIntegrity();

		TreeComparatorMeSH comparator = new TreeComparatorMeSH(source, target, "complex all");
		comparator.determineModifications();
		
		System.out.println(comparator.toString());
		
		// apply to source
		TreeModificator treeMod = new TreeModificator(source);
		treeMod.putModification(comparator);
		treeMod.applyAll();

		source.verifyIntegrity();
		
		assertTrue( TreeComparator.isEqualTrees(source, target));
		
		// export source and target
//		DataExporter.toSIF(source, outPath + "created.sif");
//    	DataExporter.toSIF(target, outPath + "target.sif");
	}
	
	/**
	 * Tests the following:
	 * <ol>
	 * 	<li> import the full MeSH 2008 as source and full MeSH 2012 as target </li> 
	 *  <li> determine modifications </li>
	 *  <li> applies modifications to source </li>
	 *  <li> check if both are equal </li>
	 * </ol>
	 * @throws Exception 
	 */
	@Test
	public void testMesh2008to2012full() throws Exception {
		
		String yearSource = "2008";
		String yearTarget = "2012";
		
		Tree source = new Tree("mesh" + yearSource);		
		DataImporter.fromOriginalMeshXml(meshPath + "desc" + yearSource + ".xml.gz", source);		
		source.verifyIntegrity();

		Tree target = new Tree("mesh" + yearTarget);
		DataImporter.fromOriginalMeshXml(meshPath + "desc" + yearTarget + ".gz", target);
		target.verifyIntegrity();

		TreeComparatorMeSH comparator = new TreeComparatorMeSH(source, target, "full MeSH " + yearSource + "->" + yearTarget);
		comparator.determineModifications();	
		
		comparator.saveModificationsToFiles(outPath + yearSource + "to" + yearTarget);
		//System.out.println(comparator.toString());

		source.verifyIntegrity();
		target.verifyIntegrity();
		
		// apply to source
		TreeModificator treeMod = new TreeModificator(source);
		treeMod.putModification(comparator);
		treeMod.applyAll(true);

		source.verifyIntegrity();
		
		assertTrue(TreeComparator.isEqualTrees(source, target));
		
		// export source and target
		DataExporter.toSIF(source, outPath + "source"+yearSource+".sif");
    	DataExporter.toSIF(target, outPath + "target"+yearTarget+".sif");
    	
    	source.printInfo(System.out);
    	target.printInfo(System.out);
	}

	/**
	 * Test easy moving - moving of a leaf of the mesh to 
	 */
	private void easyMoving() {
		Tree source = new Tree("source.xml");
		DescAdditions descs = DataImporter.fromOwnXML(testInPath + "source.xml");
		descs.apply(source);
		
		Tree easyMoving = new Tree("Test MeSH easy moving");
		descs = DataImporter.fromOwnXML(testInPath + "moving_easy_target.xml");
		descs.apply(easyMoving);
		
		TreeComparatorMeSH comparator = new TreeComparatorMeSH(source, easyMoving, "easy moving");
		comparator.determineModifications();
		
		System.out.println(comparator.toString());
		
		// the moved vertex
		String v = "V05";
		
		assertEquals( 0, comparator.getDescAdditions().size());
		assertEquals( 0, comparator.getDescDeletions().size());
		assertEquals( 0, comparator.getDescRenamings().size());
		assertEquals( 0, comparator.getVertexAdditions().size());
		assertEquals( 0, comparator.getVertexDeletions().size());
		
		VertexMovings movings = comparator.getVertexMovings();
		assertEquals( 1, movings.size());		
		
		assertTrue(movings.contains(v));
		assertEquals( "D05", movings.getOldDescUi(v));
		assertEquals( "D05", movings.getNewDescUi(v));
		assertEquals( "V02", movings.getOldParent(v));
		assertEquals( "V03", movings.getNewParent(v));
	}
	
	/**
	 * Test deep moving - moving of a vertex which is not a leaf
	 */
	private void deepMoving() {
		Tree source = new Tree("source.xml");
		DescAdditions descs = DataImporter.fromOwnXML(testInPath + "source.xml");
		descs.apply(source);
		
		Tree deepMoving = new Tree("Test MeSH deep moving");
		descs = DataImporter.fromOwnXML(testInPath + "moving_deep_target.xml");
		descs.apply(deepMoving);
		
		TreeComparatorMeSH comparator = new TreeComparatorMeSH(source, deepMoving, "deep moving");
		comparator.determineModifications();
		
		System.out.println(comparator.toString());
		
		// the moved vertex
		String v = "V02";
		
		assertEquals( 0, comparator.getDescAdditions().size());
		assertEquals( 0, comparator.getDescDeletions().size());
		assertEquals( 0, comparator.getDescRenamings().size());
		assertEquals( 0, comparator.getVertexAdditions().size());
		assertEquals( 0, comparator.getVertexDeletions().size());
		
		VertexMovings movings = comparator.getVertexMovings();
		assertEquals( 1, movings.size());			
		
		assertTrue(movings.contains(v));
		assertEquals( "D02", movings.getOldDescUi(v));
		assertEquals( "D02", movings.getNewDescUi(v));
		assertEquals( "V01", movings.getOldParent(v));
		assertEquals( "V03", movings.getNewParent(v));
	}
	
	/**
	 * test vertex addition 
	 */
	private void vertexAddition() {
		Tree source = new Tree("source.xml");
		DescAdditions descs = DataImporter.fromOwnXML(testInPath + "source.xml");
		descs.apply(source);
		
		Tree vertexAddition = new Tree("Test MeSH vertex addition");
		descs = DataImporter.fromOwnXML(testInPath + "vertex_addition_target.xml");
		descs.apply(vertexAddition);
		
		TreeComparatorMeSH comparator = new TreeComparatorMeSH(source, vertexAddition, "vertex additions");
		comparator.determineModifications();
		
		System.out.println(comparator.toString());
		
		// the added vertices
		String v6 = "V06";
		String v7 = "V07";
		String v8 = "V08";		
		
		assertEquals( 0, comparator.getDescAdditions().size());
		assertEquals( 0, comparator.getDescDeletions().size());
		assertEquals( 0, comparator.getDescRenamings().size());
		assertEquals( 0, comparator.getVertexMovings().size());
		assertEquals( 0, comparator.getVertexDeletions().size());
		
		VertexAdditions additions = comparator.getVertexAdditions();		
		assertEquals(3, additions.size());			
		
		assertTrue(additions.contains(v6));
		assertEquals( "D04", additions.getDescUi(v6));
		assertEquals( "V03", additions.getParentVertexName(v6));
		
		assertTrue(additions.contains(v7));
		assertEquals( "D01", additions.getDescUi(v7));
		assertEquals( "V03", additions.getParentVertexName(v7));

		assertTrue(additions.contains(v8));
		assertEquals( "D02", additions.getDescUi(v8));
		assertEquals( "V07", additions.getParentVertexName(v8));
	}
	
	/**
	 * test vertex deletion 1
	 */
	private void vertexDeletion_easy() {
		Tree source = new Tree("source.xml");
		DescAdditions descs = DataImporter.fromOwnXML(testInPath + "source.xml");
		descs.apply(source);
		
		Tree vertexDeletion = new Tree("Test MeSH vertex deletions");
		descs = DataImporter.fromOwnXML(testInPath + "vertex_deletion_target_easy.xml");
		descs.apply(vertexDeletion);
		
		TreeComparatorMeSH comparator = new TreeComparatorMeSH(source, vertexDeletion, "vertex deletions easy");
		comparator.determineModifications();
		
		System.out.println(comparator.toString());
		
		// the deleted vertices (v4,v5 implicitly by recursive deletion)
		String v2 = "V02";
		
		assertEquals( 0, comparator.getDescAdditions().size());
		assertEquals( 0, comparator.getDescRenamings().size());
		assertEquals( 0, comparator.getVertexMovings().size());
		assertEquals( 0, comparator.getVertexAdditions().size());
		
		DescDeletions descDeletions = comparator.getDescDeletions();
		assertEquals( 3, descDeletions.size());			
		assertTrue(descDeletions.contains("D02"));
		assertTrue(descDeletions.contains("D04"));
		assertTrue(descDeletions.contains("D05"));
		
		VertexDeletions vertexDeletions = comparator.getVertexDeletions();
		assertEquals( 1, vertexDeletions.size());			
		assertTrue(vertexDeletions.containsKey(v2));
		assertEquals(true, vertexDeletions.get(v2));
	}
	
	/**
	 * test vertex deletion 2
	 */
	private void vertexDeletion_complex() {
		Tree source = new Tree("vertex_deletion_source_complex.xml");
		DescAdditions descs = DataImporter.fromOwnXML(testInPath + "vertex_deletion_source_complex.xml");
		descs.apply(source);
		
		Tree vertexDeletion = new Tree("Test MeSH vertex deletions");
		descs = DataImporter.fromOwnXML(testInPath + "vertex_deletion_target_complex.xml");
		descs.apply(vertexDeletion);
		
		TreeComparatorMeSH comparator = new TreeComparatorMeSH(source, vertexDeletion, "vertex deletions complex");
		comparator.determineModifications();
		
		System.out.println(comparator.toString());
		
		// the deleted vertices (v4,v5 implicitly by recursive deletion)
		String v2 = "V02";
		String v6 = "V06";
		
		assertEquals( 0, comparator.getDescAdditions().size());
		assertEquals( 0, comparator.getDescRenamings().size());
		assertEquals( 0, comparator.getVertexMovings().size());
		assertEquals( 0, comparator.getVertexAdditions().size());

		DescDeletions descDeletions = comparator.getDescDeletions();
		assertEquals(3, descDeletions.size());			
		assertTrue(descDeletions.contains("D02"));
		assertTrue(descDeletions.contains("D04"));
		assertTrue(descDeletions.contains("D06"));
		
		VertexDeletions deletions = comparator.getVertexDeletions();
		assertEquals(2, deletions.size());
		
		assertTrue(deletions.containsKey(v2));
		assertEquals(true, deletions.get(v2));
		
		assertTrue(deletions.containsKey(v2));
		assertEquals(true, deletions.get(v6));
	}
	
	/**
	 * test descriptor addition
	 */
	private void descriptorAddition() {
		// just do vertexDeletion_easy the other way around ...!
		
		Tree source = new Tree("vertex_deletion_target_easy.xml");
		DescAdditions descs = DataImporter.fromOwnXML(testInPath + "vertex_deletion_target_easy.xml");
		descs.apply(source);
		
		Tree descsAdditions = new Tree("source.xml");
		descs = DataImporter.fromOwnXML(testInPath + "source.xml");
		descs.apply(descsAdditions);
		
		TreeComparatorMeSH comparator = new TreeComparatorMeSH(source, descsAdditions, "descriptor additions");
		comparator.determineModifications();
		
		System.out.println(comparator.toString());
		
		assertEquals( 0, comparator.getDescDeletions().size());
		assertEquals( 0, comparator.getDescRenamings().size());
		assertEquals( 0, comparator.getVertexMovings().size());
		assertEquals( 0, comparator.getVertexDeletions().size());
		
		DescAdditions descAdditions = comparator.getDescAdditions();
		assertEquals(3,descAdditions.size());
		
		VertexAdditions additions = comparator.getVertexAdditions();
		assertEquals(3, additions.size());		
		
		assertTrue(additions.contains("V02"));
		assertEquals( "D02", additions.getDescUi("V02"));
		assertEquals( "V01", additions.getParentVertexName("V02"));
		
		assertTrue(additions.contains("V04"));
		assertEquals( "D04", additions.getDescUi("V04"));
		assertEquals( "V02", additions.getParentVertexName("V04"));

		assertTrue(additions.contains("V05"));
		assertEquals( "D05", additions.getDescUi("V05"));
		assertEquals( "V02", additions.getParentVertexName("V05"));
	}

	/**
	 * test descriptor renamings - i.e. change of descriptor UI
	 * test vertex rebinding - i.e. vertex changed which descriptor it belongs to
	 */
	private void descriptorRenamingsAndRebindings() {
		Tree source = new Tree("source.xml");
		DescAdditions descs = DataImporter.fromOwnXML(testInPath + "source.xml");
		descs.apply(source);

		Tree descRenameRebind = new Tree("Tdesc_rebind_rename_target.xml");
		descs = DataImporter.fromOwnXML(testInPath + "desc_rebind_rename_target.xml");
		descs.apply(descRenameRebind);

		TreeComparatorMeSH comparator = new TreeComparatorMeSH(source, descRenameRebind, "desc renamings and rebindings");
		comparator.determineModifications();

		System.out.println(comparator.toString());

		// the deleted descriptor
		String d2 = "D02";

		// rebound vertex (moved)
		String v2 = "V02";

		// renamed descriptor
		String d3 = "D03"; // old name
		String d9 = "D09"; // new name

		assertEquals( 0, comparator.getDescAdditions().size());
		assertEquals( 0, comparator.getVertexAdditions().size());
		assertEquals( 0, comparator.getVertexDeletions().size());
		
		DescDeletions descDels= comparator.getDescDeletions();		
		assertEquals(1, descDels.size());
		assertTrue(descDels.contains(d2));
		
		DescRenamings renamings = comparator.getDescRenamings();
		assertEquals(1, renamings.size());
		assertTrue(renamings.containsOld(d3));
		assertTrue(renamings.containsNew(d9));

		VertexMovings movings = comparator.getVertexMovings();
		assertEquals(movings.size(), 1);
		assertTrue(movings.contains(v2));
		assertEquals( "D02", movings.getOldDescUi(v2));
		assertEquals( "D04", movings.getNewDescUi(v2));
		assertEquals( "V01", movings.getOldParent(v2));
		assertEquals( "V01", movings.getNewParent(v2));
	}
	
	private void testComplexAll() {
		Tree source = new Tree("complex_source.xml");
		DescAdditions descs = DataImporter.fromOwnXML(testInPath + "complex_source.xml");
		descs.apply(source);
		source.verifyIntegrity();
		
		Tree complexTarget = new Tree("complex_target.xml");
		descs = DataImporter.fromOwnXML(testInPath + "complex_target.xml");
		descs.apply(complexTarget);
		complexTarget.verifyIntegrity();
		
		TreeComparatorMeSH comparator = new TreeComparatorMeSH(source, complexTarget, "complex all");
		comparator.determineModifications();
		
		System.out.println(comparator.toString());
		
//		DescAdditions descAdditions = comparator.getDescAdditions();
//		assertEquals(1, descAdditions.size());		
//		assertTrue(descAdditions.containsByUI(""));
//		assertTrue(descAdditions.getByUI("").has(""));
//		
//		DescDeletions descDeletions = comparator.getDescDeletions();
//		assertEquals(1, descDeletions.size());			
//		assertTrue(descDeletions.contains(""));
//		assertTrue(descDeletions.contains(""));
//		assertTrue(descDeletions.contains(""));
//		
//		DescRenamings descRenamings = comparator.getDescRenamings();
//		assertEquals(1, descRenamings.size());
//		assertTrue(descRenamings.containsOld(""));
//		assertTrue(descRenamings.containsNew(""));
//		
//		VertexAdditions vertexAdditions = comparator.getVertexAdditions();		
//		assertEquals(1, vertexAdditions.size());			
//		assertTrue(vertexAdditions.contains(""));
//		assertEquals( vertexAdditions.getDescUi(""));
//		assertEquals(vertexAdditions.getParentVertexName(""));
//
//		VertexMovings vertexMovings = comparator.getVertexMovings();
//		assertEquals(1,vertexMovings.size());
//		assertTrue(vertexMovings.contains(""));
//		assertEquals( "", vertexMovings.getOldDescUi(""));
//		assertEquals( "", vertexMovings.getNewDescUi(""));
//		assertEquals( "", vertexMovings.getOldParent(""));
//		assertEquals( "", vertexMovings.getNewParent(""));
//		
//		VertexDeletions vertexDeletions = comparator.getVertexDeletions();
//		assertEquals(vertexDeletions.size(), 1);			
//		assertTrue(vertexDeletions.containsKey(""));
//		assertEquals(true,vertexDeletions.get(""));
		
	}
	
}

