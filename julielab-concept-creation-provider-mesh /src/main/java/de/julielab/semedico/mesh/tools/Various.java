package de.julielab.semedico.mesh.tools;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

import de.julielab.semedico.mesh.Tree;
import de.julielab.semedico.mesh.components.Descriptor;
import de.julielab.semedico.mesh.components.TreeVertex;

/**
 * This class contains various methods whose functionality is mostly independent
 * and serves a different purpose that the rest of the methods and classes in
 * this the mesh package.
 * 
 * @author Philipp Lucas
 * 
 */
public class Various {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(Various.class);

	/**
	 * <p>
	 * As the name suggests this method tests if in the given data there is any
	 * pair of descriptors which has more than one pair of tree-numbers which
	 * are in a child-parent relation.
	 * </p>
	 * 
	 * <p>
	 * This is important to know when trying determine the modification which
	 * led to the old UD MeSH. If there is not such pair then it should be
	 * possible to uniquely and correctly determine the modifications.
	 * </p>
	 * 
	 * @param data
	 *            The Tree instance to test
	 * @return Returns true if there is such a pair as described above; false
	 *         otherwise.
	 */
	public static boolean testForParentChildDescriptorPairUnambiguousness(
			Tree data) {
		logger.info("Starting to test for vertex-parent-child unambigousness of descriptor-pairs...");
		boolean flag = false;
		
		int conflictCnt = 0;
		for (Descriptor parentDesc : data.getAllDescriptors()) {
			Set<Descriptor> childDescs = new HashSet<Descriptor>(); // HERE
			
			for (TreeVertex parentVertex : parentDesc.getTreeVertices()) {				
				for (TreeVertex childVertex : data.childVerticesOf(parentVertex)) {
					
					Descriptor childDesc = data.getDescriptorByVertex(childVertex);
					
					if (childDescs.contains(childDesc)) {
						// note that the first childDesc will not be printed out in any way.
						logger.warn("parent: " + parentVertex.getName() + "@"
								+ parentDesc.getName() + "  --- child: "
								+ childVertex.getName() + "@"
								+ childDesc.getName());
						flag = true;	
						conflictCnt++;
					}
					childDescs.add(childDesc);
				}
				
			}
			logger.info(" -- next Descriptor --");
		}
		logger.info("I found " + conflictCnt + " such pairs.");
		data.printInfo(System.out);
		logger.info("... Done.");
		return flag;
	}
	
	/**
	 * <p>
	 * As the name suggests this method tests if in the given data there is any
	 * pair of a tree-vertex 'parent' and descriptor 'child' so that there is
	 * more than one tree-vertex of 'child' which has 'parent' as its parent
	 * vertex.
	 * </p>
	 * 
	 * <p>
	 * This is important to know when trying determine the modification which
	 * led to the old UD MeSH. If there is not such pair then it should be
	 * possible to uniquely and correctly determine the modifications.
	 * </p>
	 * 
	 * @param data
	 *            The Tree instance to test
	 * @return Returns true if there is such a pair as described above; false
	 *         otherwise.
	 */
	public static boolean testForParentVertexChildDescriptorPairUnambiguousness(Tree data) {
		logger.info("Starting to test for vertex-parent-child unambigousness of descriptor-pairs...");
		boolean flag = false;
		
		int conflictCnt = 0;
		for (Descriptor parentDesc : data.getAllDescriptors()) 			{
			for (TreeVertex parentVertex : parentDesc.getTreeVertices()) {
				
				Set<Descriptor> childDescs = new HashSet<Descriptor>(); // HERE
				
				for (TreeVertex childVertex : data.childVerticesOf(parentVertex)) {
					
					Descriptor childDesc = data.getDescriptorByVertex(childVertex);
					
					if (childDescs.contains(childDesc)) {
						// note that the first childDesc will not be printed out in any way.
						logger.warn("parent: " + parentVertex.getName() + "@"
								+ parentDesc.getName() + "  --- child: "
								+ childVertex.getName() + "@"
								+ childDesc.getName());
						flag = true;	
						conflictCnt++;
					}
					childDescs.add(childDesc);
				}
				
			}
			logger.info(" -- next Descriptor --");
		}		
		logger.info("I found " + conflictCnt + " such pairs.");
		data.printInfo(System.out);
		logger.info("... Done.");
		return flag;
	}
	
}


