/**
 * CategoryAttachment.java
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
 * Creation date: 08.03.2012
 **/

/**
 * 
 */
package de.julielab.semedico.mesh.descriptorAttachment;

import java.util.List;

import com.google.common.collect.Lists;

import de.julielab.semedico.mesh.components.TreeVertex;

/**
 * A RecursiveAttachment is a triple of an attachment string, the distance from
 * the vertex which originally passed on the attachment and this original tree
 * vertex itself.
 * 
 * @author faessler
 * 
 */
public class RecursiveAttachment extends Attachment {

	public static final int OP_FULL = 1;
	public static final int OP_WO_DIST = 2;

	private List<TreeVertex> pathFromAttachmentOrigin;
	private int outputMode;

	public RecursiveAttachment(String category,	List<TreeVertex> pathFromAttachmentOrigin) {
		super(category);
		this.pathFromAttachmentOrigin = pathFromAttachmentOrigin;
		this.outputMode = OP_FULL;
	}

	/**
	 * @return the category
	 */
	public String getCategory() {
		return attachment;
	}

	/**
	 * @return the distance
	 */
	public int getDistance() {
		return pathFromAttachmentOrigin.size() - 1;
	}

	/**
	 * @return the categoryOrigin
	 */
	public List<TreeVertex> getPathFromAttachmentOrigin() {
		return pathFromAttachmentOrigin;
	}

	/**
	 * @return the outputMode
	 */
	public int getOutputMode() {
		return outputMode;
	}

	/**
	 * @param outputMode
	 *            the outputMode to set
	 */
	public void setOutputMode(int outputMode) {
		this.outputMode = outputMode;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.julielab.stemnet.mesh2.descriptorAttachment.Attachment#toString()
	 */
	@Override
	public String toString() {
		String str = null;
		switch (outputMode) {
		case OP_FULL:
			str = getCategory() + "(" + getDistance() + ")";
			break;
		case OP_WO_DIST:
			str = getCategory();
			break;
		}
		return str;
	}
}
