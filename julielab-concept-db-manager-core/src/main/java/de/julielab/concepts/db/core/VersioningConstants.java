package de.julielab.concepts.db.core;

import static de.julielab.concepts.db.core.ConfigurationConstants.VERSION;

public class VersioningConstants {
	public static final String CREATE_VERSION = "CREATE (v:VERSION {version: $version})";
	public static final String CREATE_UNIQUE_CONSTRAINT = "CREATE CONSTRAINT ON (v:VERSION) ASSERT v." + VERSION + " IS UNIQUE";
	public static final String GET_VERSION = "MATCH (v:VERSION) return v." + VERSION + " AS " + VERSION;
	
	private VersioningConstants() {
	}
}
