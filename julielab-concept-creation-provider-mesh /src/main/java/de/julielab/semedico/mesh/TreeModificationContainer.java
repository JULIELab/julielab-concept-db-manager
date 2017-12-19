package de.julielab.semedico.mesh;

import org.slf4j.Logger;

import de.julielab.semedico.mesh.exchange.ModificationExporter;
import de.julielab.semedico.mesh.modifications.DescAdditions;
import de.julielab.semedico.mesh.modifications.DescDeletions;
import de.julielab.semedico.mesh.modifications.DescRelabellings;
import de.julielab.semedico.mesh.modifications.DescRenamings;
import de.julielab.semedico.mesh.modifications.VertexAdditions;
import de.julielab.semedico.mesh.modifications.VertexDeletions;
import de.julielab.semedico.mesh.modifications.VertexMovings;
import de.julielab.semedico.mesh.modifications.VertexRenamings;

/**
 * Container for <code>Tree</code> modifications that allows easy storing and
 * retrieval.
 * 
 * @author Philipp Lucas
 * 
 */
public class TreeModificationContainer {

	private static Logger logger = org.slf4j.LoggerFactory.getLogger(TreeModificationContainer.class);
	
	/* variables that store all modifications */
	protected DescAdditions descAdditions;
	protected DescRenamings descRenamings;
	protected DescRelabellings descRelabellings;
	protected DescDeletions descDeletions;
	protected VertexMovings vertexMovings; 
	protected VertexDeletions vertexDeletions;
	protected VertexAdditions vertexAdditions;
	protected VertexRenamings vertexRenamings;
	
	// a name for the container
	private String name;
	
	public TreeModificationContainer() {
		name = "no-name";
		init();
	}
	
	/**
	 * Like the standard constructor, but we can give a name to the container.
	 */
	public TreeModificationContainer(String name) {
		this.name = name;
		init();
	}
	
	private void init() {
		descAdditions = new DescAdditions();
		descRenamings = new DescRenamings();
		descRelabellings = new DescRelabellings();
		descDeletions = new DescDeletions();
		vertexMovings = new VertexMovings();
		vertexDeletions = new VertexDeletions();
		vertexAdditions = new VertexAdditions();
		vertexRenamings = new VertexRenamings();
	}
	
	/**
	 * @Return Returns the name of this container.
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * @return Returns all added descriptors.
	 */
	public DescAdditions getDescAdditions() {
		return descAdditions;
	}

	/**
	 * @return Returns all renamed descriptors.
	 */
	public DescRenamings getDescRenamings() {
		return descRenamings;
	}	
	
	/**
	 * @return Returns all renamed descriptors.
	 */
	public DescRelabellings getDescRelabellings() {
		return descRelabellings  ;
	}	
	
	/**
	 * @return Returns the set of all those descriptors which are part of
	 *         <code>data1</code> but not of <code>data2<code>.
	 */
	public DescDeletions getDescDeletions() {
		return descDeletions;
	}

	/**
	 * @return Returns all vertex additions that have been deleted
	 *         from <code>source</code>.
	 */
	public VertexAdditions getVertexAdditions() {
		return vertexAdditions;
	}
	
	/**
	 * @return Returns a set of all those tree vertices that have been deleted
	 *         from <code>source</code>.
	 */
	public VertexDeletions getVertexDeletions() {
		return vertexDeletions;
	}
	
	/**
	 * @return Returns all vertex movings.
	 */
	public VertexMovings getVertexMovings() {
		return vertexMovings;
	}
	
	/**
	 * @return Returns all vertex renamings.
	 */
	public VertexRenamings getVertexRenamings() {
		return vertexRenamings;
	}
	
	public void putModification(DescAdditions descAdditions) {
		this.descAdditions = descAdditions;
	}
	
	public void putModification(DescRenamings descRenamings) {
		this.descRenamings = descRenamings;
	}
	
	public void putModification(DescRelabellings descRelabellings) {
		this.descRelabellings = descRelabellings ;
	}
	
	public void putModification(DescDeletions descDeletions) {
		this.descDeletions = descDeletions;
	}
	
	public void putModification(VertexAdditions vertexAdditions) {
		this.vertexAdditions = vertexAdditions;
	}
	
	public void putModification(VertexDeletions vertexDeletions) {
		this.vertexDeletions = vertexDeletions;
	}
	
	public void putModification(VertexMovings vertexMovings) {
		this.vertexMovings = vertexMovings;
	}
	
	public void putModification(VertexRenamings vertexRenamings) {
		this.vertexRenamings = vertexRenamings;
	}
	
	/**
	 * Puts all modifications in <code>modsContainer</code> in this object.
	 * @param modsContainer A modification container.
	 */
	public void putModification(TreeModificationContainer comparator) {
		descAdditions = comparator.getDescAdditions();
		vertexAdditions = comparator.getVertexAdditions();
		vertexMovings = comparator.getVertexMovings();
		descRenamings = comparator.getDescRenamings();
		descRelabellings = comparator.getDescRelabellings();
		vertexDeletions = comparator.getVertexDeletions();
		descDeletions = comparator.getDescDeletions();
		vertexRenamings = comparator.getVertexRenamings();
	}
	
	/**
	 * <p>
	 * Saves all determined modifications to a number of files. Will only save if the modification object isn't <code>null</code>.
	 * <ul>
	 * <li>This list isn't up to date!!</li>
	 * <li>new descriptors as XML file to: <code>baseFileName + "_DescAdds.xml"</code></li>
	 * <li>descriptor renamings to: <code>baseFileName + "_DescRenamings.csv"</code></li>
	 * <li>descriptor relabellings to: <code>baseFileName + "_DescRelabellings.csv"</code></li>
	 * <li>descriptors deletions as CSV file to <code>baseFileName</code><code>_deletedDescs.csv</code></li>
	 * <li>vertex additions to: <code>baseFileName + "_VertexAdditions.csv"</code></li>
	 * <li>moved tree vertices as CSV file to: <code>baseFileName</code><code>_VertexMovings.csv</code></li>
	 * <li>vertex renamings to: <code>baseFileName + "_VertexRenamings.csv"</code></li>
	 * <li>deleted vertices as CSV file to <code>baseFileName</code><code>_deletedVertices.csv</code></li>
	 * <li>deleted vertices per descriptor as CSV file to <code>baseFileName</code><code>_deletedVerticesPerDesc.csv</code></li>
	 * </ul>
	 * </p>
	 * <p>
	 * Format: The format of all these files are simple self-explanatory csv
	 * files. However, the format of the XML file describing new descriptors is
	 * MeSH XML (or a subset of it).
	 * </p>
	 * 
	 * @param baseFileName
	 *            The base file name for the file namings...
	 */
	public void saveModificationsToFiles(String baseFileName) {
		logger.info("# Saving modification operations of " + getName() + " to files ... ");		
		ModificationExporter.saveDescAdditions(descAdditions, baseFileName + "_DescAdds.xml");
		ModificationExporter.saveDescRenamings(descRenamings, baseFileName + "_DescRenamings.csv");
		ModificationExporter.saveDescRelabellings(descRelabellings, baseFileName + "_DescRelabellings.csv");
		ModificationExporter.saveDescDeletions(descDeletions, baseFileName + "_DescDeletions.csv");
		ModificationExporter.saveVertexAdditions(vertexAdditions, baseFileName + "_VertexAdditions.csv");
		ModificationExporter.saveVertexDeletions(vertexDeletions, baseFileName);
		ModificationExporter.saveVertexMovings(vertexMovings, baseFileName + "_VertexMovings.csv");
		ModificationExporter.saveVertexRenamings(vertexRenamings, baseFileName + "_VertexRenamings.csv");
		logger.info("# ... done. ");
	}
}
