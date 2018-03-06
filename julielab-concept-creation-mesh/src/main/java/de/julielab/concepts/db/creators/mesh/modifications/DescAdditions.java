package de.julielab.concepts.db.creators.mesh.modifications;

import java.util.LinkedHashMap;

import org.slf4j.Logger;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.VertexLocations;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;

/**
 * <p>
 * A class to handle descriptor additions. It stores them and can apply them to
 * a <code>Tree</code> object.
 * </p>
 * 
 * <p>
 * Note: adding 'descriptor additions' to this via <code>addLocation</code> or
 * <code>put</code> will add a <i>copy</i> of the descriptor to this mapping!
 * </p>
 * 
 * <p>
 * In the future: Note: this can NOT store new vertices of new descriptors. Use <code>VertexAdditions</code> for that.
 * </p>.
 * 
 * @author Philipp Lucas
 */

public class DescAdditions
extends LinkedHashMap<Descriptor, VertexLocations>
implements TreeModficationsInterface {

	private static final long serialVersionUID = 1L;
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(DescAdditions.class);

	// New descriptors with locations.
	// Maps descriptors to the locations of their tree-vertices. 
	//private Map<Descriptor, VertexLocations> newDescs2Locations = new HashMap<Descriptor, VertexLocations>();
	// for another way of accessing this data
	private LinkedHashMap<String, Descriptor> ui2desc = new LinkedHashMap<String, Descriptor>(); 
	
	/**
	 * Adds the descriptors currently hold in this object to <code>data</code>.
	 * @param data A <code>Tree</code> object.
	 */
	public void apply(Tree data) {
		logger.info("# Adding descriptors to " + data.getName() + " ...");
		ProgressCounter counter = new ProgressCounter(size(), 10, "descriptor");
		for(Descriptor desc : keySet()) {
			data.addDescriptor(desc, get(desc));
			counter.inc();
		}
		logger.info("# Done.");
	}


	/**
	 * Adds this mapping to the map. Overwrites existing mappings!
	 * 
	 * <p>Note: for this purpose descriptors equal if their UI equals.
	 * Note: this will add a <i>copy</i> of <code>desc</code> to this mapping!</p>
	 */
	@Override
	public VertexLocations put(Descriptor desc, VertexLocations locs) {
		Descriptor descCopy = new Descriptor(desc, false);

		// remove previous mapping if existing
		String descUI = desc.getUI();
		if (ui2desc.containsKey(descUI)) {
			logger.warn("Overwriting existing descriptor mapping. Old descriptor was : " + ui2desc.get(descUI).toString() );
			removeByUi(descUI);
		}			
		
		super.put(descCopy, locs);
		ui2desc.put(descUI, descCopy);
		return locs;
	}
	
	/**
	 * Adds a location to the set of locations for <code>desc</code>. It also
	 * works if no location for <code>desc</code> exists yet.
	 * 
	 * Note: this will add a <i>copy</i> of <code>desc</code> to this mapping!
	 */
	public void addLocation(Descriptor desc, String vertexName, String parentVertexName) {
		String descUi = desc.getUI();
		// new entry or not?
		if ( ui2desc.containsKey(descUi)) {
			VertexLocations locs = get(ui2desc.get(descUi));
			locs.put(vertexName, parentVertexName);
		} else {
			put(desc, new VertexLocations(vertexName,parentVertexName));
		} 
	}
	
	public boolean contains(Descriptor desc) {
		return ui2desc.containsKey(desc.getUI());
	}	
	
	@Override
	public String toString() {
		StringBuffer out = new StringBuffer();
		for(Descriptor desc : keySet()) {
			out.append(desc.toString() + "\n");
		}
		return out.toString();
	}
	
	public String toString(String descUi) {
		return ui2desc.get(descUi).toString();
	}
	
	public VertexLocations remove (Descriptor desc) {
		return removeByUi(desc.getUI());
	}
	
	public VertexLocations removeByUi(String descUi) {
		Descriptor desc_internal = ui2desc.get(descUi);
		if (desc_internal == null) {
			return null;
		}
		ui2desc.remove(descUi);
		return super.remove(desc_internal);
	}

	public boolean containsByUI(String descUi) {
		return ui2desc.containsKey(descUi);
	}

	public VertexLocations getByUI (String descUi) {
		Descriptor desc = ui2desc.get(descUi);
		if(desc == null) {
			return null;
		} else {
			return super.get(desc);
		}
	}
	
	
	public VertexLocations get(Descriptor desc) {
		return getByUI(desc.getUI());
	}

}
