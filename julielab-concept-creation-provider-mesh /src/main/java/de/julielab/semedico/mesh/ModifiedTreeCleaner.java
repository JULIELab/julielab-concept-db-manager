package de.julielab.semedico.mesh;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.semedico.mesh.components.Descriptor;
import de.julielab.semedico.mesh.components.TreeVertex;

public class ModifiedTreeCleaner {

	private static final Logger log = LoggerFactory.getLogger(ModifiedTreeCleaner.class);

	private Tree modifiedMesh;
	private Tree udMesh;

	public ModifiedTreeCleaner(Tree meshCurrent, Tree udMesh2008) {
		this.modifiedMesh = meshCurrent;
		this.udMesh = udMesh2008;
	}

	/**
	 * The children of the root node should only be facets. But when terms in the MeSH get either moved up as a branch
	 * root or are entirely new, they will be here as well. We cut them.
	 * 
	 * @param data
	 */
	public void clean() {
		Set<Descriptor> rootChildren = new LinkedHashSet<>(modifiedMesh.childDescriptorsOf(modifiedMesh.getRootDesc()));
		FacetsProvider facetsUd = new FacetsProvider(udMesh);

		List<TreeVertex> verticesToCut = new ArrayList<>();
		for (Descriptor rootChild : rootChildren) {
			if (rootChild.getName().startsWith("Facet"))
				;// System.out.println(facet.getUI() + " is facet");
			else if (null != udMesh.getDescriptorByUi(rootChild.getUI())) {
				// This term existed in the original Semedico MeSH but seems to be confused regarding its correct
				// location.
				// We will now locate the original parent in the modified MeSH and move the whole branch there, if
				// possible.
				Descriptor originalDesc = udMesh.getDescriptorByUi(rootChild.getUI());
				for (Descriptor parentDesc : udMesh.parentDescriptorsOf(originalDesc)) {
					Descriptor parentInModifiedMesh = modifiedMesh.getDescriptorByUi(parentDesc.getUI());
					if (null != parentInModifiedMesh) {
						// Append the homeless descriptor to the descriptor with the same UI as its original parent.
						List<TreeVertex> modifiedParentVertices = parentInModifiedMesh.getTreeVertices();
						if (!modifiedParentVertices.isEmpty()) {
							for (TreeVertex vertexModifiedParent : modifiedParentVertices) {
								for (TreeVertex vertexHomeless : rootChild.getTreeVertices()) {
									modifiedMesh.moveBranch(vertexHomeless, vertexModifiedParent);
								}
							}
						} else {
							// Somehow, in the new MeSH the old parent has no tree vertices. I don't know how this is
							// possible. As a quick fix, append it to its original facet.
							Set<String> facetNames = facetsUd.getFacets(parentDesc);
							for (String facetName : facetNames) {
								Descriptor facetDesc = modifiedMesh.getDescriptorByName(facetName);
								TreeVertex facetVertex = facetDesc.getTreeVertices().iterator().next();
								for (TreeVertex vertexHomeless : rootChild.getTreeVertices()) {
									modifiedMesh.moveBranch(vertexHomeless, facetVertex);
								}

							}
						}
					} else {
						log.warn("Parent of "
								+ originalDesc.getUI()
								+ " does not exist in modified mesh! Parent: "
								+ parentDesc.getUI()
								+ ". You have to go to this class and think about what to do, since this case did not occur in the past.");
					}
				}
				;// System.out.println(facet.getUI() + " existed before");
			} else if (null == udMesh.getDescriptorByUi(rootChild.getUI())) {
				// logger.info(facet.getUI() + " did not exist before and is removed");
				for (TreeVertex vertex : rootChild.getTreeVertices())
					verticesToCut.add(vertex);

			}
		}
		for (TreeVertex vertexToCut : verticesToCut) {
			modifiedMesh.cutBranch(vertexToCut);
		}
	}
}
