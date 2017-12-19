package de.julielab.semedico.mesh.modifications;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

import org.slf4j.Logger;

import de.julielab.semedico.mesh.Tree;

/**
 * <p>
 * Abstract class representing renamings.
 * </p> * 
 * <p>
 * A renaming is a pair consisting of:
 * <ul>
 * <li>key = the old name</li>
 * <li>value = the new name</li>
 * </ul>
 * </p>
 * 
 * @author Philipp Lucas
 * 
 */
public abstract class Renamings<Old,New> 
implements TreeModficationsInterface {
	
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(Renamings.class);
	private LinkedHashMap<Old,New> old2new = new LinkedHashMap<Old,New>();
	private LinkedHashMap<New,Old> new2old= new LinkedHashMap<New,Old>();

	/**
	 * Applies the renamings to <code>data</code>.
	 * 
	 * @param data
	 *            A <code>Tree</code> object.
	 */
	abstract public void apply(Tree data);
	
	public void put(Old o, New n) {
		boolean overwriteFlag = false;
		// check for existing mappings
		if (new2old.containsKey(n)) {
			logger.warn("Overwriting existing renaming ");
			logger.warn(toString4New(n) + "    with");
			new2old.remove(n);
			overwriteFlag = true;
		}
		if (old2new.containsKey(o)) {
			logger.warn("Overwriting existing renaming ");
			logger.warn(toString4Old(o) + "    with");
			old2new.remove(o);
			overwriteFlag = true;
		}
				
		old2new.put(o,n);		
		new2old.put(n,o);
		
		if(overwriteFlag)
			logger.warn(toString4Old(o));
	}
	
	public New getNew(Old o) {
		return old2new.get(o);
	}
	
	public Old getOld(New n) {
		return new2old.get(n);
	}
	
	/**
	 * @return Returns <code>true</code> iff <code>containsOld(o) && getNew(o).equals(n)</code>, i.e. if this contains a renaming for <code>o</code> to <code>n</code>.    
	 */
	public boolean contains(Old o, New n) {
		return containsOld(o) && getNew(o).equals(n);
	}
	
	public boolean containsOld(Old o) {
		return old2new.containsKey(o);
	}
	
	public boolean containsNew(New n) {
		return new2old.containsKey(n);
	}
	
	public Set<Old> getOldSet() {
		return old2new.keySet();
	}
	
	public Set<New> getNewSet() {
		return new2old.keySet();
	}
	
	public int size() {
		return old2new.size();
	}
	
	public String toString4Old(Object o) {
		return "old = " + o.toString() + " --- " + "new = " + old2new.get(o).toString();
	}
	
	public String toString4New(New n) {
		return toString4Old(new2old.get(n));
	}
	
	public String toString() {
		StringBuffer out = new StringBuffer();
		for( Old o : old2new.keySet()) {
			 out.append( toString4Old(o) + "\n");
		}
		return out.toString();
	}
	
	/**
	 * Removes ALL unnecessary renamings, i.e. mappings that equal() in key and value.
	 */
	public void removeUnnecessary() {
		// collect all to remove
		Set<Old> store = new HashSet<Old>();
		for (Old o : getOldSet()) {
			if ( getNew(o).equals(o)) {
				store.add(o);
			}
		}
		
		// remove them
		for(Old o : store) {
			removeByOld(o);
		}
	}
	
	/**
	 * Removes the given renaming.
	 * @param o
	 * @param n
	 * @return True if renaming was contained and removed, false otherwise.
	 */
	public boolean remove(Old o, New n) {
		if (contains(o,n)) {
			removeByOld(o);
			return true;
		}
		return false;
	}
	
	public New removeByOld(Old o) {
		New n = old2new.remove(o);
		new2old.remove(n);
		return n;
	}
	
	public Old removeByNew(New n) {
		Old o = new2old.remove(n);
		old2new.remove(o);
		return o;
	}
	
	public boolean isEmpty() {
		return old2new.isEmpty();
	}

}
