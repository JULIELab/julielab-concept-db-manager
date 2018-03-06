/**
 * CategoryAttacher.java
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
package de.julielab.concepts.db.creators.mesh.descriptorAttachment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;

/**
 * <p>
 * Class to attach external categorizations to MeSH descriptors. It is assumed
 * that an attached category of a descriptor should be passed on to its
 * children.
 * </p>
 * <p>
 * Given a MeSH tree structure and a mapping of MeSH descriptor UIs to a set of
 * predefined categories, these categories are attached to the corresponding
 * descriptors (just by using a map). Then, the categories are also attached to
 * child descriptors (and their children etc.). In case a descriptor inherits
 * multiple categories, the categories are ordered ascending regarding the
 * distance of the term which has passed on the respective category. <samp> TODO
 * </samp>
 * </p>
 * 
 * @author faessler
 * 
 */
public class RecursiveAttacher extends BaseAttacher {

	private static Logger logger = org.slf4j.LoggerFactory
			.getLogger(RecursiveAttacher.class);

	public RecursiveAttacher(Tree data) {
		super(data);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.julielab.stemnet.mesh2.descriptorAttachment.DirectAttacher#
	 * addAttachmentToDescriptor(java.util.List,
	 * de.julielab.stemnet.mesh2.Descriptor)
	 */
	@Override
	protected void addAttachmentToDescriptor(List<String> categories,
			Descriptor descriptor) {
		for (TreeVertex vertex : descriptor.getTreeVertices()) {
			for (String category : categories)
				treeVertexAttachments.put(
						vertex,
						getRecursiveAttachment(category,
								Lists.newArrayList(vertex)));
			passAttachmentsToChildren(vertex, vertex,
					Lists.newArrayList(vertex), data);
		}
	}

	/**
	 * Recursive method passing down the category of
	 * <code>originalDescriptor</code>.
	 * 
	 * @param parentDescriptor
	 * @param recursionDepth
	 * @param pathFromAttachmentOrigin
	 * @param data
	 */
	private void passAttachmentsToChildren(TreeVertex parentVertex,
			TreeVertex attachmentOrigin,
			List<TreeVertex> pathFromAttachmentOrigin, Tree data) {

		List<Attachment> originalAttachments = treeVertexAttachments
				.get(attachmentOrigin);

		Descriptor parentDescriptor = data.getDescriptorByUi(parentVertex
				.getDescUi());

		for (TreeVertex parentDescriptorVerticesVertex : parentDescriptor
				.getTreeVertices()) {
			for (TreeVertex childVertex : data
					.childVerticesOf(parentDescriptorVerticesVertex)) {
				List<TreeVertex> childBranch = new ArrayList<TreeVertex>(pathFromAttachmentOrigin.size());
				for (TreeVertex v : pathFromAttachmentOrigin)
					childBranch.add(v);
				childBranch.add(childVertex);

				for (Attachment attachment : originalAttachments) {
					RecursiveAttachment catAttachment = (RecursiveAttachment) attachment;
					String category = catAttachment.getCategory();
					treeVertexAttachments.put(
							childVertex,
							getRecursiveAttachment(category,
									childBranch));
				}

				passAttachmentsToChildren(childVertex, attachmentOrigin,
						childBranch, data);
			}
		}
	}

	private RecursiveAttachment getRecursiveAttachment(String attachmentString,
			List<TreeVertex> pathFromAttachmentOrigin, int outputMode) {
		RecursiveAttachment ret = new RecursiveAttachment(attachmentString,
				pathFromAttachmentOrigin);
		ret.setOutputMode(outputMode);
		return ret;
	}

	private RecursiveAttachment getRecursiveAttachment(String attachmentString,
			List<TreeVertex> pathFromAttachmentOrigin) {
		return getRecursiveAttachment(attachmentString,
				pathFromAttachmentOrigin, RecursiveAttachment.OP_FULL);
	}

	private Comparator<RecursiveAttachment> distanceComparator = new Comparator<RecursiveAttachment>() {
		@Override
		public int compare(RecursiveAttachment o1, RecursiveAttachment o2) {
			return new Integer(o1.getDistance()).compareTo(new Integer(o2
					.getDistance()));
		}
	};

	/**
	 * <p>
	 * Cleans a list of recursive attachments.
	 * </p>
	 * <p>
	 * First, for each string attachment only one attachment instance is kept,
	 * viz. the one with shortest distance to the category origin. Then, the
	 * attachment objects are sorted ascending by this distance.
	 * </p>
	 * 
	 * @param attachmentsForVertices
	 * @return
	 */
	@Override
	protected List<? extends Attachment> cleanUpAttachments(
			List<Attachment> attachmentsForVertices) {
		List<RecursiveAttachment> cleanedAttachments = new ArrayList<RecursiveAttachment>();

		// Firstly, sort attachments by category.
		Collections.sort(attachmentsForVertices, new Comparator<Attachment>() {
			@Override
			public int compare(Attachment arg0, Attachment arg1) {
				return arg0.getAttachment().compareTo(arg1.getAttachment());
			}
		});

		// Now, for each category only copy the attachment which minimum
		// distance to category origin in the result list.
		String currentCategory = null;
		int bestCategoryIndex = 0;
		for (Attachment attachment : attachmentsForVertices) {
			RecursiveAttachment recAttachment = (RecursiveAttachment) attachment;
			if (currentCategory != null) {
				// When the current category changes, we save the best outcome
				// (shortest distance) of the previous category and move the
				// index
				// "pointer" to the position for the next best category
				// attachment.
				if (!currentCategory.equals(recAttachment.getAttachment())) {
					{
						bestCategoryIndex++;
						cleanedAttachments.add(recAttachment);
					}
				}
				// If we have a shorter distance within the same category,
				// change the reference attachment.
				if (currentCategory.equals(recAttachment.getAttachment())
						&& recAttachment.getDistance() < cleanedAttachments
								.get(bestCategoryIndex).getDistance()) {
					cleanedAttachments.set(bestCategoryIndex, recAttachment);
				}
			}
			// Initialization.
			else
				cleanedAttachments.add(recAttachment);

			currentCategory = recAttachment.getCategory();
		}

		// Eventually, sort by distance.
		Collections.sort(cleanedAttachments, distanceComparator);

		if (attachmentsForVertices != null && attachmentsForVertices.size() > 0
				&& cleanedAttachments.size() == 0)
			logger.error("Deleted all attachments instead of unifying!");

		return cleanedAttachments;
	}

}
