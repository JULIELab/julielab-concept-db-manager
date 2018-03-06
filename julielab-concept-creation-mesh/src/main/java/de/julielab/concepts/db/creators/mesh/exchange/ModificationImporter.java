package de.julielab.concepts.db.creators.mesh.exchange;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

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
 * For import tree-modifications from files.
 * 
 * It can import all modification classes in the <code>modifications</code>
 * package.
 * 
 * @author Philipp Lucas
 * 
 */
public class ModificationImporter {

	private static final String DELIM = ",\t";

	private static Logger logger = org.slf4j.LoggerFactory
			.getLogger(ModificationImporter.class);
	
	/**
	 * Imports 'added' descriptors from <code>fileName</code> and returns them. 
	 * 
	 * @param fileName
	 *            Name of file to import from.
	 */
	public static DescAdditions importDescAdditions(String fileName){
		logger.info("Importing descriptor additions from " + fileName + " ...");
		DescAdditions newDesc = DataImporter.fromOwnXML(fileName);
		logger.info(" ... done.");
		return newDesc;
	}
	
	/**
	 * Imports descriptor deletions from <code>fileName</code> and returns them.
	 * 
	 * @param fileName  Name of file to import from.
	 */
	public static DescDeletions importDescDeletions(String fileName) {
		ProgressCounter counter = new ProgressCounter(0, 50, "desc deletion");
		DescDeletions dels = new DescDeletions();
		try {
			logger.info("Importing desc-deletions from " + fileName
					+ " ...");
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String[] values;
			while (reader.ready()) {
				
				String line = reader.readLine().trim();
				
				if (!line.startsWith("!") && !line.isEmpty()) {
					values = line.split(DELIM);
					String descUi= values[0];
					
					if(values.length != 1) {
						logger.warn("Skipping invalid line in file " + fileName + ". line was: " + line);
						continue;						
					} else {
						dels.add(descUi);
						counter.inc();
					}
				}
			}
			reader.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return dels;
	}
	
	/**
	 * Imports descriptor renamings from <code>fileName</code> and returns them.
	 * 
	 * @param fileName  Name of file to import from.
	 */
	public static DescRenamings importDescRenamings(String fileName) {
		ProgressCounter counter = new ProgressCounter(0, 50, "desc renamings");
		DescRenamings renamings = new DescRenamings();
		try {
			logger.info("Importing desc-renamings from " + fileName
					+ " ...");
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String[] values;
			while (reader.ready()) {
				
				String line = reader.readLine().trim();
				
				if (!line.startsWith("!") && !line.isEmpty()) {
					values = line.split(DELIM);
					String descUiOld= values[0];
					String descUiNew= values[1];
					
					if(values.length != 2) {
						logger.warn("Skipping invalid line in file " + fileName + ". line was: " + line);
						continue;						
					} else {
						renamings.put(descUiOld, descUiNew);
						counter.inc();
					}
				}
			}
			reader.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return renamings;
	}
	
	/**
	 * Imports descriptor relabellings from <code>fileName</code> and returns them.
	 * 
	 * @param fileName  Name of file to import from.
	 */
	public static DescRelabellings importDescRelabellings(String fileName) {
		ProgressCounter counter = new ProgressCounter(0, 50, "desc renamings");
		DescRelabellings relabellings = new DescRelabellings();
		try {
			logger.info("Importing desc-relabellings from " + fileName
					+ " ...");
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String[] values;
			while (reader.ready()) {
				
				String line = reader.readLine().trim();
				
				if (!line.startsWith("!") && !line.isEmpty()) {
					values = line.split(DELIM);
					String descUiOld= values[0];
					String descUiNew= values[1];
					
					if(values.length != 2) {
						logger.warn("Skipping invalid line in file " + fileName + ". line was: " + line);
						continue;						
					} else {
						relabellings.put(descUiOld, descUiNew);
						counter.inc();
					}
				}
			}
			reader.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return relabellings;
	}
	
	/**
	 * Imports 'added' vertices (to existing descriptors) from <code>fileName</code> and returns them. 
	 * 
	 * @param fileName
	 *            Name of file to import from.
	 */
	public static VertexAdditions importVertexAdditions(String fileName){
		ProgressCounter counter = new ProgressCounter(0, 50, "vertex");
		VertexAdditions adds = new VertexAdditions();
		try {
			logger.info("Importing tree-vertex additions from " + fileName
					+ " ...");
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String[] values;
			while (reader.ready()) {
				
				String line = reader.readLine().trim();
				
				if (!line.startsWith("!") && !line.isEmpty()) {
					values = line.split(DELIM);
					String vertexName = values[0];
					String parentVertexName = values[1]; 
					String descUi = values[2];
					
					if(values.length != 5) {
						logger.warn("Skipping invalid line in file " + fileName + ". line was: " + line);
						continue;						
					} else {
						adds.put(vertexName, parentVertexName, descUi);
						counter.inc();
					}
				}
			}
			reader.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return adds;
	}


	/**
	 * Imports vertex-deletion-modifications from <code>fileName</code> and returns them. 
	 * 
	 * @param fileName
	 *            Name of file to import from.
	 */
	public static VertexDeletions importVertexDeletions(String fileName) {
		VertexDeletions vertexDeletions = new VertexDeletions();
		ProgressCounter counter = new ProgressCounter(0, 1000, "vertex");
		try {
			logger.info("Importing deletions from " + fileName + " ...");
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String[] values;
			while (reader.ready()) {
				
				String line = reader.readLine().trim();
				
				if (!line.startsWith("!") && !line.isEmpty()) {
					values = line.split(DELIM);
					if (values.length != 2) {
						logger.warn("Skipping invalid line in file " + fileName
								+ ". line was: " + line);
						continue;
					}
					String vertexName = values[0];					
					vertexDeletions.put(vertexName, Boolean.parseBoolean(values[1]));
					counter.inc();
				}
			}
			reader.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return vertexDeletions;
	}
	
	
	/**
	 * Imports vertex-moving-modifications from <code>fileName</code> and returns them. 
	 * 
	 * @param fileName
	 *            Name of file to import from.
	 */
	public static VertexMovings importVertexMovings(String fileName){
		ProgressCounter counter = new ProgressCounter(0, 50, "vertex");
		VertexMovings movings = new VertexMovings();
		try {
			logger.info("Importing tree-vertex movings from " + fileName
					+ " ...");
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String[] values;
			while (reader.ready()) {
				
				String line = reader.readLine().trim();
				
				if (!line.startsWith("!") && !line.isEmpty()) {
					values = line.split(DELIM);
					String vertexName = values[0];
					String sourceVertexName = values[1]; 
					String targetVertexName = values[2];
					String sourceDescUi = values[3];
					String targetDescUi = values[4];
					
					if(values.length != 5) {
						logger.warn("Skipping invalid line in file " + fileName + ". line was: " + line);
						continue;						
					} else {
						movings.put(vertexName, sourceVertexName, targetVertexName, sourceDescUi, targetDescUi);
						counter.inc();
					}
				}
			}
			reader.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return movings;
	}
	
	
	/**
	 * Imports vertex-moving-modifications from <code>fileName</code> and returns them. 
	 * 
	 * @param fileName
	 *            Name of file to import from.
	 */
	public static VertexRenamings importVertexRenamings(String fileName){
		ProgressCounter counter = new ProgressCounter(0, 50, "vertex renamings");
		VertexRenamings renamings = new VertexRenamings();
		try {
			logger.info("Importing tree-vertex renamings from " + fileName
					+ " ...");
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String[] values;
			while (reader.ready()) {
				
				String line = reader.readLine().trim();
				
				if (!line.startsWith("!") && !line.isEmpty()) {
					values = line.split(DELIM);
					String oldName = values[0];
					String newName = values[1];

					if(values.length != 2) {
						logger.warn("Skipping invalid line in file " + fileName + ". line was: " + line);
						continue;						
					} else {
						renamings.put(oldName,newName);
						counter.inc();
					}
				}
			}
			reader.close();
			logger.info(" ... done.");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return renamings;
	}


}
