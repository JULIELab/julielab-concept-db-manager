package de.julielab.concepts.db.creators.mesh.exchange;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.components.Concept;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.Term;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.components.VertexLocations;
import de.julielab.concepts.db.creators.mesh.modifications.DescAdditions;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * This class deals with exporting a <code>Tree</code> object into various
 * formats and/or targets.
 * 
 * @author Philipp Lucas
 */
public class DataExporter {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(DataExporter.class);

	/**
	 * Exports tree into a file at file path in the DOT format.
	 * 
	 * @param tree
	 *            Tree to export.
	 * @param filepath
	 *            File path to save the exported data at.
	 */
	static public void toDOT(Tree tree, String filepath) {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	/**
	 * Exports tree into easy to understand file format.
	 * 
	 * @param tree
	 *            Tree to export.
	 * @param filepath
	 *            File path to save the exported data at.
	 */
	static public void toOwnTxt(Tree tree, String filepath) {
		logger.info("# Exporting data to own simple file format '" + filepath + "' ... ");

		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filepath));

			// get all descriptors
			Collection<Descriptor> allDesc = tree.getAllDescriptors();
			ProgressCounter counter = new ProgressCounter(allDesc.size(), 10, "descriptor");
			counter.startMsg();

			for (Descriptor desc : allDesc) {
				writer.write(desc.tofullString(tree));
				writer.write("\n");
				counter.inc();
			}
			writer.close();

		} catch (IOException e) {
			logger.error("Error writing to file: " + e.getMessage());
		}

		logger.info("# ... done.");
	}

	/**
	 * <p>
	 * Exports the descriptors <code>descs</code> to file <code>filename</code>.
	 * Format will be "OwnXML" format.
	 * </p>
	 * 
	 * <p>
	 * The locations provided by <code>desc2locations</code> will be used
	 * instead of any other possible real locations of the descriptors, in case
	 * the descriptors are part of a tree. Note that there is no need for a
	 * descriptor to be part of a tree.
	 * </p>
	 * 
	 * @param locations
	 *            The locations for each descriptor.
	 * @param descs
	 *            A set of descriptors of <code>tree</code>
	 * @param filename
	 *            A filename to export the descriptors to.
	 */
	static public void toOwnXml(DescAdditions desc2locations, String filename) {
		try {
			Set<Descriptor> descsSortedList = desc2locations.keySet();
			// sort collection by descriptor name
			// List<Descriptor> descsSortedList = new
			// ArrayList<Descriptor>(desc2locations.keySet());
			// Collections.sort(descsSortedList, new
			// DescriptorNameComparator());

			logger.info("# Exporting " + desc2locations.size() + " descriptors to ownXML file '" + filename + "' ... ");

			OutputStream out = new FileOutputStream(filename);
			OutputStream bufferedOut = new BufferedOutputStream(out);
			XMLOutputFactory factory = XMLOutputFactory.newInstance();
			// factory.setProperty(OutputKeys.INDENT, "yes"); // doesn't work
			XMLStreamWriter writer = factory.createXMLStreamWriter(bufferedOut, "UTF-8");

			writer.writeStartDocument("UTF-8", "1.0");
			writer.writeStartElement("DescriptorRecordSet");

			ProgressCounter counter = new ProgressCounter(descsSortedList.size(), 10, "descriptor");
			for (Descriptor desc : descsSortedList) {
				writeDescToOwnXml(desc2locations.get(desc), desc, writer);
				counter.inc();
			}

			writer.writeEndElement();
			writer.writeEndDocument();

			writer.flush();
			writer.close();
			bufferedOut.close();
			out.close();

			logger.info("# ... done.");
		} catch (XMLStreamException e) {
			System.err.println(e.getStackTrace());
			System.err.println(e.getMessage());
		} catch (FileNotFoundException e) {
			System.err.println("File '" + filename + "' not found.");
		} catch (IOException e) {
			System.err.println("general IO Exception: " + e.getMessage());
		}
	}

	/**
	 * Exports all descriptors in <code>data</code> to file
	 * <code>filename</code>. Format will be "OwnXML".
	 * 
	 * @param data
	 *            A <code>Tree</code> object.
	 * @param filename
	 *            A filename to export the descriptors to.
	 */
	static public void toOwnXml(Tree data, String filename) {
		DescAdditions descAdds = new DescAdditions();
		for (Descriptor desc : data.getAllDescriptors()) {
			VertexLocations locs = new VertexLocations();
			descAdds.put(desc, locs);
			for (TreeVertex v : desc.getTreeVertices()) {
				TreeVertex parent = data.parentVertexOf(v);

				if (parent == null) {
					locs.put(v.getName(), null);
				} else {
					locs.put(v.getName(), parent.getName());
				}
			}
		}
		toOwnXml(descAdds, filename);
	}

	/**
	 * Writes out the given descriptor to the writer...
	 * 
	 * @param data
	 * @param desc
	 * @param writer
	 * @throws XMLStreamException
	 */
	static private void writeDescToOwnXml(VertexLocations locations, Descriptor desc, XMLStreamWriter writer)
			throws XMLStreamException {
		writer.writeStartElement("DescriptorRecord");
		writer.writeCharacters("\n");

		// DescriptorUI
		writer.writeStartElement("DescriptorUI");
		writer.writeCharacters(desc.getUI());
		writer.writeEndElement();
		writer.writeCharacters("\n");

		// Locations
		writer.writeStartElement("LocationList");
		writer.writeCharacters("\n");
		for (String vertexName : locations.getVertexNameSet()) {
			writer.writeStartElement("Location");
			writer.writeCharacters("\n");

			writer.writeStartElement("VertexName");
			writer.writeCharacters(vertexName);
			writer.writeEndElement();
			writer.writeCharacters("\n");

			String parentName = locations.get(vertexName);
			if (parentName != null && !parentName.isEmpty()) {
				writer.writeStartElement("ParentVertexName");
				writer.writeCharacters(parentName);
				writer.writeEndElement();
				writer.writeCharacters("\n");
			}

			writer.writeEndElement();
			writer.writeCharacters("\n");
		}
		writer.writeEndElement();
		writer.writeCharacters("\n");

		// Concepts & Terms
		writer.writeStartElement("ConceptList");
		writer.writeCharacters("\n");
		for (Concept concept : desc.getConcepts()) {
			writer.writeStartElement("Concept");
			writer.writeAttribute("PreferredConceptYN", (concept.isPreferred() ? "Y" : "N"));
			writer.writeCharacters("\n");

			writer.writeStartElement("ScopeNote");
			writer.writeCharacters(desc.getScopeNote());
			writer.writeEndElement();
			writer.writeCharacters("\n");

			writer.writeStartElement("TermList");
			writer.writeCharacters("\n");
			for (Term term : concept.getTerms()) {
				writer.writeStartElement("Term");
				writer.writeAttribute("ConceptPreferredTermYN", (term.isPreferred() ? "Y" : "N"));
				writer.writeCharacters("\n");

				// writer.writeStartElement("TermUI");
				// writer.writeCharacters(term.getID());
				// writer.writeEndElement(); writer.writeCharacters("\n");

				writer.writeStartElement("String");
				writer.writeCharacters(term.getName());
				writer.writeEndElement();
				writer.writeCharacters("\n");

				writer.writeEndElement();
				writer.writeCharacters("\n");
			}
			writer.writeEndElement();
			writer.writeCharacters("\n");

			writer.writeEndElement();
			writer.writeCharacters("\n");
		}
		writer.writeEndElement();
		writer.writeCharacters("\n");

		writer.writeEndElement();
		writer.writeCharacters("\n");
	}


	/**
	 * Determines the facet of <code> vertex </code>. We do it like this:
	 * <ol>
	 * <li>we go up in the hierarchy till we get to a child of root</li>
	 * <li>we use the the first character of that vertex's original tree number
	 * to determine the facet ID</li>
	 * </ol>
	 * 
	 * @param vertex
	 *            A tree vertex.
	 * @param tree
	 *            A tree instance.
	 * @return Returns the facet ID of vertex.
	 */
	private static int getFacetID(TreeVertex vertex, Tree tree) throws IllegalArgumentException {

		if (vertex.equals(tree.getRootVertex())) {
			new IllegalArgumentException("root vertex cannot be an argument of this method.");
		}

		// go up till we get to a child of root
		TreeVertex root = tree.getRootVertex();
		TreeVertex parent = tree.parentVertexOf(vertex);
		while (!parent.equals(root)) {
			vertex = parent;
			parent = tree.parentVertexOf(vertex);
		}

		// use thats vertex original tree number
		int facet_id = -2;
		switch (vertex.getName().charAt(0)) {
		case 'A':
			facet_id = 23;
			break;
		case 'B':
			facet_id = 24;
			break;
		case 'C':
			facet_id = 25;
			break;
		case 'D':
			facet_id = 26;
			break;
		case 'E':
			facet_id = 27;
			break;
		case 'F':
			facet_id = 28;
			break;
		case 'G':
			facet_id = 29;
			break;
		case 'H':
			facet_id = 30;
			break;
		case 'I':
			facet_id = 31;
			break;
		case 'J':
			facet_id = 32;
			break;
		case 'K':
			facet_id = 33;
			break;
		case 'L':
			facet_id = 34;
			break;
		case 'M':
			facet_id = 35;
			break;
		case 'N':
			facet_id = 36;
			break;
		case 'V':
			facet_id = 37;
			break;
		default:
			throw new IllegalArgumentException(
					"Tree Vertex '" + vertex.toString() + "' does not belong to the Ageing-MeSH.");
		}
		return facet_id;
	}

	/**
	 * Exports tree into a file at file path in the SIF format.
	 * 
	 * @param tree
	 *            Tree to export.
	 * @param filepath
	 *            File path to save the exported data at.
	 */
	static public void toSIF(Tree tree, String filepath) {

		logger.info("# Exporting data to SIF file '" + filepath + "' ... ");

		try {
			Writer fw = new BufferedWriter(new FileWriter(filepath));

			ProgressCounter counter = new ProgressCounter(tree.vertexSet().size(), 10, "tree vertex");
			counter.startMsg();

			// Breadh first traversion of tree
			TreeSet<String> sortSet = new TreeSet<String>();
			BreadthFirstIterator<TreeVertex, DefaultEdge> iter = new BreadthFirstIterator<TreeVertex, DefaultEdge>(
					tree);
			while (iter.hasNext()) {
				TreeVertex v = (TreeVertex) iter.next();
				// TreeNumber treeNr = tree.treeNumberOf(v);
				// get all children and save edges with these in SIF file
				Set<DefaultEdge> outEdges = tree.outgoingEdgesOf(v);
				for (DefaultEdge e : outEdges) {
					TreeVertex child = (TreeVertex) tree.getEdgeTarget(e);
					// /TreeNumber childNr = tree.treeNumberOf(child);
					// write to file
					sortSet.add(v.getName() + " pointsAt " + child.getName() + "\n");
				}
				counter.inc();
			}

			// write out (thanks to TreeSet it is sorted!)
			for (String str : sortSet) {
				fw.write(str);
			}

			fw.close();

		} catch (IOException e) {
			logger.error("Error writing to SIF file: " + e.getMessage());
		}

		logger.info("# ... done.");
	}

}