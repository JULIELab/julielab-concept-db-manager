package de.julielab.concepts.db.core;

public class VersioningConstants {
	public static final String CONFKEY_VERSION = "version";
	
	public static final String PROP_VERSION = "version";
	public static final String CREATE_VERSION = "CREATE (v:VERSION {version: {version}})";
	public static final String CREATE_UNIQUE_CONSTRAINT = "CREATE CONSTRAINT ON (v:VERSION) ASSERT v." + PROP_VERSION + " IS UNIQUE";
	public static final String GET_VERSION = "MATCH (v:VERSION) return v." + PROP_VERSION + " AS " + PROP_VERSION;
	
	private VersioningConstants() {
	}
}
