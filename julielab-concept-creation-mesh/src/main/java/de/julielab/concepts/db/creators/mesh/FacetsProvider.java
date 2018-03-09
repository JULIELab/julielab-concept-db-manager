package de.julielab.concepts.db.creators.mesh;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetGroup;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;

/**
 * Provides access to facets of a <code>Tree</code> instance.
 * 
 * <p>
 * Please note that facets are only an implicit feature, i.e. it is only meaningful to use this class, if there are
 * actually any facets in the Tree. This is because facets are only a convention of structure, rather than actually
 * specific object.
 * </p>
 * 
 * <p>
 * More specifically, by convention, facets are all direct children of the root node. Their names are the names of the
 * corresponding descriptor.
 * </p>
 * 
 * <p>
 * When importing fromt the UD-MeSH there will be facets according to the xml files. However, the facet names will be
 * preceeded by "Facet ". For example, if the actual facet name specified in the xml is "foobar" then the facet
 * descriptor name will be "Facet foobar". All right?
 * </p>
 * 
 * @author Philipp Lucas
 * 
 */

public class FacetsProvider {

	private Tree tree;

	/**
	 * @param tree
	 *            Tree of which to provide facets.
	 */
	public FacetsProvider(Tree tree) {
		this.tree = tree;
	}

	/**
	 * @return Returns the name of the facet of the given TreeVertex, or null if there is no such TreeVertex in the
	 *         Tree, or if v is the root vertex..
	 */
	public String getFacet(TreeVertex v) {
		TreeVertex root = tree.getRootVertex();

		if (!tree.hasVertex(v.getName()) || v.equals(root)) {
			return null;
		}

		TreeVertex parent = tree.parentVertexOf(v);

		if (parent.equals(root)) {
			return v.getName();
		} else {
			return getFacet(parent);
		}
	}

	/**
	 * @return Returns a set containing all facets names of all tree vertices of descriptor <code>d</code>.
	 */
	public Set<String> getFacets(Descriptor d) {
		// We use a linked set to get some order stability between different runs of the algorithm
		Set<String> set = new LinkedHashSet<String>();

		for (TreeVertex v : d.getTreeVertices()) {
			String facet = getFacet(v);
			if (facet != null) {
				set.add(facet);
			}
		}

		return set;
	}

	/**
	 * @return Returns true if the facet of <code>v</code> equals <code>facet</code>; false else.
	 */
	public boolean hasFacet(TreeVertex v, String facet) {
		String realFacet = getFacet(v);

		if (realFacet != null && realFacet.equals(facet)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * @return Returns true if <code>d</code> has facet <code>facet</code>. Note that a descriptor may have multiple
	 *         facets.
	 */
	public boolean hasFacet(Descriptor d, String facet) {

		for (TreeVertex v : d.getTreeVertices()) {
			if (hasFacet(v, facet)) {
				return true;
			}
		}
		return false;
	}

//	public static ImportFacet createSemedicoImportFacet(String facetname) {
//		// To avoid naming collisions, facets may have the naming prefix "Facet"
//		String facetName = facetname.replaceFirst("Facet ", "");
//		String cssId = null;
//		String facetGroupName = null;
//		Integer position = null;
//		String sourceType = FacetConstants.SRC_TYPE_HIERARCHICAL;
//		List<String> facetGroupGeneralLabels = new ArrayList<>();
//		List<String> facetUniqueLabels = new ArrayList<>();
//		List<String> facetGeneralLabels = new ArrayList<>();
//
//		Integer facetGroupPosition = null;
//
////		List<String> searchFieldNames =
////				Lists.newArrayList(IIndexInformationService.TITLE, IIndexInformationService.ABSTRACT,
////						IIndexInformationService.MESH);
//
//		facetGeneralLabels.add(FacetLabels.General.USE_FOR_SUGGESTIONS.toString());
//		facetGeneralLabels.add(FacetLabels.General.USE_FOR_QUERY_DICTIONARY.toString());
//		facetGeneralLabels.add(FacetLabels.General.USE_FOR_BTERMS.toString());
//
//		if ("Genes and Proteins".equals(facetName)) {
//			cssId = "proteins";
//			facetGroupName = "BioMed";
//			position = 0;
//		} else if ("Chemicals and Drugs".equals(facetName)) {
//			cssId = "chemicals";
//			facetGroupName = "BioMed";
//			position = 1;
//		} else if ("Diseases / Pathological Processes".equals(facetName)) {
//			cssId = "diseases";
//			facetGroupName = "BioMed";
//			position = 2;
//		} else if ("Organisms".equals(facetName)) {
//			cssId = "organisms";
//			facetGroupName = "BioMed";
//			position = 3;
//		} else if ("Cellular Processes".equals(facetName)) {
//			cssId = "cellularProcesses";
//			facetGroupName = "BioMed";
//			position = 4;
//		} else if ("Investigative Techniques".equals(facetName)) {
//			cssId = "techniques";
//			facetGroupName = "BioMed";
//			position = 5;
//		} else if ("Gene Expression".equals(facetName)) {
//			cssId = "geneExression";
//			facetGroupName = "BioMed";
//			position = 6;
//		} else if ("Signs and Symptoms".equals(facetName)) {
//			cssId = "signs";
//			facetGroupName = "BioMed";
//			position = 7;
//		} else if ("Therapies and Treatments".equals(facetName)) {
//			cssId = "therapies";
//			facetGroupName = "BioMed";
//			position = 8;
//		} else
//		// ----------- THE REST ARE IMMUNOLOGY RELATED FACETS ----------------
//		if ("Immunoglobulins and Antibodies".equals(facetName)) {
//			cssId = "antibodies";
//			facetGroupName = "BioMed";
//			position = 15;
//			// position = 1;
//		} else if ("Minor Histocompatibility Antigens".equals(facetName)) {
//			cssId = "mha";
//			facetGroupName = "BioMed";
//			position = 15;
//			// position = 2;
//		} else if ("Hematopoietic Progenitor Cells".equals(facetName)) {
//			cssId = "progenitors";
//			facetGroupName = "BioMed";
//			position = 15;
//			// position = 3;
//		} else if ("Blood Cells".equals(facetName)) {
//			cssId = "bloodCells";
//			facetGroupName = "BioMed";
//			position = 15;
//			// position = 4;
//		} else if ("Epitopes and Binding Sites".equals(facetName)) {
//			cssId = "epitopes";
//			facetGroupName = "BioMed";
//			position = 15;
//			// position = 5;
//		} else if ("Immune Processes".equals(facetName)) {
//			cssId = "immunity";
//			facetGroupName = "BioMed";
//			position = 15;
//			// position = 6;
//		} else if ("Transplantation".equals(facetName)) {
//			cssId = "transplantation";
//			facetGroupName = "BioMed";
//			position = 15;
//			// position = 7;
//		} else if ("Concepts".equals(facetName)) {
//			// This case won't actually be needed in the future since the old "Concepts" - i.e. terms that were not
//			// displayed in the tree structure of any facets and thus were only used for suggestions and query parsing -
//			// are not used any more in favor of the BioPortal terms that contain everything that was contained in these
//			// Concepts before. Those were 10250 MeSH terms that did not seem to fit into one of the facets. But we
//			// import the whole MeSH now, anyway.
//			cssId = "concepts";
//			facetGroupName = "NoFacet";
//			facetUniqueLabels.add(FacetLabels.Unique.NO_FACET.toString());
//			position = 0;
//		} else {
//			throw new IllegalArgumentException("Facet with name \"" + facetName + "\" is unknown.");
//		}
//
//		if (facetGroupName.equals("BioMed")) {
//			facetGroupPosition = 0;
//			facetGroupGeneralLabels.add(FacetGroupLabels.General.SHOW_FOR_SEARCH.toString());
//			// Not for the moment
//			// facetGroupGeneralLabels.add(FacetGroupLabels.General.SHOW_FOR_BTERMS.toString());
//		} else {
//			facetGroupPosition = -1;
//		}
//
//		ImportFacetGroup importFacetGroup =
//				new ImportFacetGroup(facetGroupName, facetGroupPosition, facetGroupGeneralLabels);
//
//		ImportFacet importFacet =
//				new SemedicoImportFacet(facetName, cssId, sourceType, Collections.<String>emptyList(), Collections.<String> emptyList(),
//						position, facetGeneralLabels, importFacetGroup);
//		importFacet.setLabels(facetUniqueLabels);
//
//		return importFacet;
//	}

//	public static boolean isImmunologyFacet(String facetName) {
//		String effectiveFacetName = facetName.replaceFirst("Facet ", "");
//		switch (effectiveFacetName) {
//		case "Immunoglobulins and Antibodies":
//		case "Minor Histocompatibility Antigens":
//		case "Hematopoietic Progenitor Cells":
//		case "Blood Cells":
//		case "Epitopes and Binding Sites":
//		case "Immune Processes":
//		case "Transplantation":
//			return true;
//		}
//		return false;
//	}

	public static ImportFacet createMeshImportFacet(String facetname) {
		// To avoid naming collisions, facets may have the naming prefix "Facet"
		String cssId =  "mesh_" + facetname;
		String facetGroupName = "MeSH";
		Integer position = null;
		String sourceType = FacetConstants.SRC_TYPE_HIERARCHICAL;
		List<String> facetGroupGeneralLabels = new ArrayList<>();
		List<String> facetUniqueLabels = new ArrayList<>();
		List<String> facetGeneralLabels = new ArrayList<>();

		Integer facetGroupPosition = null;


		String facetName = null;
		switch (facetname) {
		case "A":
			facetName = "Anatomy";
			position = 0;
			break;
		case "B":
			facetName = "Organisms";
			position = 1;
			break;
		case "C":
			facetName = "Diseases";
			position = 2;
			break;
		case "D":
			facetName = "Chemicals and Drugs";
			position = 3;
			break;
		case "E":
			facetName = "Analytical, Diagnostic and Therapeutic Techniques and Equipment";
			position = 4;
			break;
		case "F":
			facetName = "Psychiatry and Psychology";
			position = 5;
			break;
		case "G":
			facetName = "Phenomena and Processes";
			position = 6;
			break;
		case "H":
			facetName = "Disciplines and Occupations";
			position = 7;
			break;
		case "I":
			facetName = "Anthropology, Education, Sociology and Social Phenomena";
			position = 8;
			break;
		case "J":
			facetName = "Technology, Industry, Agriculture";
			position = 9;
			break;
		case "K":
			facetName = "Humanities";
			position = 10;
			break;
		case "L":
			facetName = "Information Science";
			position = 11;
			break;
		case "M":
			facetName = "Named Groups";
			position = 12;
			break;
		case "N":
			facetName = "Health Care";
			position = 13;
			break;
		case "V":
			facetName = "Publication Characteristics";
			position = 14;
			break;
		case "Z":
			facetName = "Geographicals";
			position = 15;
			break;
		default:
			throw new IllegalArgumentException("Facet with name \"" + facetName + "\" is unknown.");
		}


		ImportFacetGroup importFacetGroup =
				new ImportFacetGroup(facetGroupName, facetGroupPosition, facetGroupGeneralLabels);

		ImportFacet importFacet =
				new ImportFacet(importFacetGroup, "mesh-" + facetName, facetName, null, sourceType, Collections.emptyList(), false); 
		importFacet.setLabels(facetUniqueLabels);

		return importFacet;
	}
}
