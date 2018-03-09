package de.julielab.concepts.db.creators.mesh.exchange;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.modifications.DescAdditions;
import de.julielab.concepts.db.creators.mesh.modifications.DescDeletions;
import de.julielab.concepts.db.creators.mesh.modifications.DescRelabellings;
import de.julielab.concepts.db.creators.mesh.modifications.DescRenamings;
import de.julielab.concepts.db.creators.mesh.modifications.VertexAdditions;
import de.julielab.concepts.db.creators.mesh.modifications.VertexDeletions;
import de.julielab.concepts.db.creators.mesh.modifications.VertexMovings;
import de.julielab.concepts.db.creators.mesh.modifications.VertexRenamings;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * For exporting tree-modifications into files.
 * 
 * It can export all modification classes in the <code>modifications</code>
 * package.
 * 
 * @author Philipp Lucas
 * 
 */

public class ModificationExporter {

	private static final String DELIM = ",\t";

	private static Logger logger = org.slf4j.LoggerFactory
			.getLogger(ModificationExporter.class);

	/**
	 * Saves descriptor additions to a XML file at <code>fileName</code>.
	 * 
	 * @param desc2locations
	 *            Map that maps descriptors to their locations.
	 * @param fileName
	 *            Name of file to save to.
	 */
	public static void saveDescAdditions(DescAdditions desc2locations,
			String fileName) {
		logger.info("Saving descriptor additions to " + fileName + " ...");
		DataExporter.toOwnXml(desc2locations, fileName);
		logger.info(" ... done.");
	}

	/**
	 * Saves descriptor deletions to a csv-file at <code>fileName</code>. See
	 * source code or such a file for information about its format.
	 * 
	 * @param descDels
	 *            Descriptor deletions to save.
	 * @param fileName
	 *            Full path of file to save it in.
	 */
	public static void saveDescDeletions(DescDeletions descDels, String fileName) {
		try {
			logger.info("Saving descriptor deletions to " + fileName + " ...");
			ProgressCounter counter = new ProgressCounter(descDels.size(), 10,
					"vertex");

			Writer writer = new BufferedWriter(new FileWriter(fileName, false));
			writer.write("! This file contains one line per full descriptor deletion. \n"
					+ "! First column: desc UI of descriptor to delete. \n\n");
			for (String descUi : descDels) {
				writer.write(descUi + "\n");
				counter.inc();
			}
			writer.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	
	/**
	 * Saves descriptor renamings to a CSV-file at <code>fileName</code>. See source
	 * code or such a file for information about its format.
	 * 
	 * @param descRenamings
	 *            Descriptor renamings to save.
	 * @param fileName
	 *            Full path of file to save it in.
	 */
	public static void saveDescRenamings(DescRenamings descRenamings,
			String fileName) {
		try {
			logger.info("Saving descriptor renamings to " + fileName + " ...");
			ProgressCounter counter = new ProgressCounter(descRenamings.size(), 10, "vertex");

			Writer writer = new BufferedWriter(new FileWriter(fileName, false));
			writer.write("! This file contains one line for descriptor renaming. \n"
					+ "! First column: old descriptor UI. \n"
					+ "! Second column: new descriptor UI. \n\n");
			for (String oldDescUi : descRenamings.getOldSet()) {
				writer.write(oldDescUi + DELIM
						+ descRenamings.getNew(oldDescUi) + "\n");
				counter.inc();
			}
			writer.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * Saves descriptor relabellings to a CSV-file at <code>fileName</code>. See source
	 * code or such a file for information about its format.
	 * 
	 * @param descRelabellings
	 *            Descriptor relabellings to save.
	 * @param fileName
	 *            Full path of file to save it in.
	 */
	public static void saveDescRelabellings(DescRelabellings descRelabellings,
			String fileName) {
		try {
			logger.info("Saving descriptor renamings to " + fileName + " ...");
			ProgressCounter counter = new ProgressCounter(descRelabellings.size(), 10, "vertex");

			Writer writer = new BufferedWriter(new FileWriter(fileName, false));
			writer.write("! This file contains one line for descriptor renaming. \n"
					+ "! First column: old descriptor name. \n"
					+ "! Second column: new descriptor name. \n\n");
			for (String oldDescUi : descRelabellings.getOldSet()) {
				writer.write(oldDescUi + DELIM
						+ descRelabellings.getNew(oldDescUi) + "\n");
				counter.inc();
			}
			writer.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * Saves vertex additions to a CSV-file at <code>fileName</code>. See source
	 * code or such a file for information about it's format.
	 * 
	 * @param vertexAdditions
	 *            Vertex additions to save.
	 * @param fileName
	 *            Full path of file to save it in.
	 */
	public static void saveVertexAdditions(VertexAdditions vertexAdditions,
			String fileName) {
		try {
			logger.info("Saving tree-vertex additions to " + fileName + " ...");
			ProgressCounter counter = new ProgressCounter(
					vertexAdditions.size(), 10, "vertex");

			Writer writer = new BufferedWriter(new FileWriter(fileName, false));
			writer.write("! This file contains one line for each tree-vertex addition. \n"
					+ "! First column: vertex-name of vertex to add \n"
					+ "! Second column: parent-vertex-name of vertex to add \n"
					+ "! Third column: UI of descriptor the to be added vertex is bound to \n\n");
			for (String addedVertexName : vertexAdditions.keySet()) {
				writer.write(addedVertexName + DELIM
						+ vertexAdditions.getParentVertexName(addedVertexName)
						+ DELIM + vertexAdditions.getDescUi(addedVertexName)
						+ "\n");
				counter.inc();
			}
			writer.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * Saves vertex movings to a CSV-file at <code>fileName</code>. See source
	 * code or such a file for information about it's format.
	 * 
	 * @param vertexMovings
	 *            Vertex movings to save.
	 * @param fileName
	 *            Full path of file to save it in.
	 */
	public static void saveVertexMovings(VertexMovings vertexMovings,
			String fileName) {
		try {
			logger.info("Saving tree-vertex movings to " + fileName + " ...");
			ProgressCounter counter = new ProgressCounter(vertexMovings.size(),
					10, "vertex");

			Writer writer = new BufferedWriter(new FileWriter(fileName, false));
			writer.write("! This file contains one line for each moved tree-vertex. \n"
					+ "! First column: tree-number of moved tree-vertex. \n"
					+ "! Second column: tree-number of original parent. \n"
					+ "! Third column: tree-number of new parent. \n"
					+ "! Forth column: ui of corresponding original descriptor. \n"
					+ "! Fifth column: ui of corresponding new descriptor. \n\n");
			for (String movedVertexName : vertexMovings.keySet()) {
				writer.write(movedVertexName + DELIM
						+ vertexMovings.getOldParent(movedVertexName) + DELIM
						+ vertexMovings.getNewParent(movedVertexName) + DELIM
						+ vertexMovings.getOldDescUi(movedVertexName) + DELIM
						+ vertexMovings.getNewDescUi(movedVertexName) + "\n");
				counter.inc();
			}
			writer.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * <p>
	 * Saves deletions to CSV-files. Base file name for these files is
	 * <code>baseFileName</code>.
	 * </p>
	 * 
	 * <p>
	 * In any way vertex deletions are saved in a file at
	 * <code>baseFileName + "_deletedVertices.csv"</code. Depending on the
	 * result of <code>deletedVertices.isAdditonalDescInfos()</code> also
	 * deleted vertices per descriptor are saved in a file at
	 * <code>baseFileName + "_deletedVerticesPerDesc.csv"</code>.
	 * </p>
	 * 
	 * See source code or such a file for information about it's format.
	 * 
	 * @param deletedVertices
	 *            Vertex deletions to save.
	 * @param baseFileName
	 *            Base name of file to save to.
	 */
	public static void saveVertexDeletions(VertexDeletions deletedVertices,
			String baseFileName) {
		try {
			logger.info("Saving deletions ...");
			Writer writer;
			ProgressCounter counter = new ProgressCounter(
					deletedVertices.size(), 10, "vertex");

			/* deleted tree-vertices */
			logger.info(" ... deleted tree-vertices to " + baseFileName
					+ "_VertexDeletions.csv ...");
			writer = new BufferedWriter(new FileWriter(baseFileName
					+ "_VertexDeletions.csv", false));
			writer.write("! This file contains one line for each deleted tree-vertex.\n"
					+ "! First column: tree-number of deleted tree-vertex.\n"
					+ "! Second column: true(1) if deletion is recursive, false(0) if not.\n");
			for (String delVertexName : deletedVertices.keySet()) {
				writer.write(delVertexName + DELIM
						+ deletedVertices.get(delVertexName) + "\n");
				counter.inc();
			}
			writer.close();

			if (deletedVertices.isAdditonalDescInfos()) {
				/* deleted tree-vertices per descriptor */
				Map<String, Set<String>> delDescs = deletedVertices
						.getDelDescs();
				logger.info(" ... deleted tree-vertices per descriptor to "
						+ baseFileName + "_VertexDeletionsPerDesc.csv ...");
				writer = new BufferedWriter(new FileWriter(baseFileName
						+ "_VertexDeletionsPerDesc.csv", false));
				writer.write("! This file contains one line for each deleted tree-vertex of a descriptor. If n tree-vertices of a descriptor x have been deleted, it contains thus n lines for this descriptor. \n"
						+ "! First column: UI of descriptor of which a tree-vertex has been deleted. \n"
						+ "! Second column: tree-number of deleted tree-vertex. \n\n");
				for (String descUi : delDescs.keySet()) {
					for (String delVertexName : delDescs.get(descUi)) {
						writer.write(descUi + DELIM + delVertexName + "\n");
					}
				}
				writer.close();
			}

			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	
	/**
	 * Saves vertex renamings to a CSV-file at <code>fileName</code>. See source
	 * code or such a file for information about it's format.
	 * 
	 * <p>Note: unnecessary renamings, i.e. a renaming where new and old name match, are not saved!</p>
	 * 
	 * @param vertexRenamings
	 *            Vertex movings to save.
	 * @param fileName
	 *            Full path of file to save it in.
	 *            
	 */
	public static void saveVertexRenamings(VertexRenamings vertexRenamings,
			String fileName) {
		try {
			logger.info("Saving tree-vertex renamings to " + fileName + " ...");
			ProgressCounter counter = new ProgressCounter(vertexRenamings.size(),
					10, "vertex renaming");
			
			Writer writer = new BufferedWriter(new FileWriter(fileName, false));
			writer.write("! This file contains one line for each moved tree-vertex. \n"
					+ "! First column: original name of tree-vertex. \n"
					+ "! Second column: new name of tree-vertex. \n\n");
			for (String renamedVertexName : vertexRenamings.getOldSet()) {
				writer.write(renamedVertexName + DELIM
						+ vertexRenamings.getNew(renamedVertexName)+ "\n");
				counter.inc();
			}
			writer.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

}
