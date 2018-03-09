package de.julielab.concepts.db.creators.mesh;

import static org.junit.Assert.assertEquals;

import java.util.TreeMap;

import org.junit.Test;

import com.google.common.collect.Ordering;

import de.julielab.concepts.db.creators.mesh.tools.ValueComparableMap;

/**
 * Class for testing <code>ValueComparableMap</code.
 * @author Philipp Lucas
 *
 */
public class TestValueComparableMap {

	@Test
	public void testValueComparableMap() {
		TreeMap<String, Integer> map = new ValueComparableMap<String, Integer>(
				Ordering.natural());
		map.put("a", 5);
		map.put("b", 1);
		map.put("c", 3);
		assertEquals("b", map.firstKey());
		assertEquals("a", map.lastKey());
		map.put("d", 0);
		assertEquals("d", map.firstKey());
		// ensure it's still a map (by overwriting a key, but with a new value)
		map.put("d", 2);
		assertEquals("b", map.firstKey());
		// Ensure multiple values do not clobber keys
		map.put("e", 2);
		assertEquals(5, map.size());
		assertEquals(2, (int) map.get("e"));
		assertEquals(2, (int) map.get("d"));	
		assertEquals(null, map.get("f") );
	}
}
