/**
 * Attachment.java
 *
 * Copyright (c) 2012, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: faessler
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 16.04.2012
 **/

/**
 * 
 */
package de.julielab.concepts.db.creators.mesh.descriptorAttachment;

/**
 * @author faessler
 *
 */
public class Attachment implements Comparable<Attachment> {
	protected final String attachment;
	
	public Attachment(String attachment) {
		this.attachment = attachment;
	}

	/**
	 * @return the attachment
	 */
	public String getAttachment() {
		return attachment;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return attachment;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Attachment arg0) {
		return attachment.compareTo(arg0.attachment);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Attachment))
			return false;
		Attachment attachment = (Attachment) obj;
		return this.attachment.equals(attachment.attachment);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return attachment.hashCode();
	}
}

