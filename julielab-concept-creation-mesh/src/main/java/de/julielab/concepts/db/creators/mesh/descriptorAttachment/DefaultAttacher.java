/**
 * DefaultAttacher.java
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

import java.util.List;

import com.google.common.collect.Lists;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;

/**
 * <p>
 * A simple attacher which does just attach one default attachment to each
 * descriptor which does not have an attachment after the initial phase (i.e.
 * no attachment in the attachment file).
 * </p>
 * 
 * @author faessler
 * 
 */
public class DefaultAttacher extends BaseAttacher {

	private final List<Attachment> defaultAttachment;

	/**
	 * @param data
	 */
	public DefaultAttacher(Tree data, Attachment defaultAttachment) {
		super(data);
		this.defaultAttachment = Lists.newArrayList(defaultAttachment);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.julielab.stemnet.mesh2.descriptorAttachment.DirectAttacher#
	 * getDescriptorAttachment(de.julielab.stemnet.mesh2.Descriptor)
	 */
	@Override
	protected List<Attachment> getDescriptorAttachment(Descriptor descriptor) {
		List<Attachment> descriptorAttachment = super.getDescriptorAttachment(descriptor);
		if(0 == descriptorAttachment.size()){
			return defaultAttachment;
		}
		return descriptorAttachment;
	}

}
