/**
 * DirectAttacher.java
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;

/**
 * <p>
 * Simple attacher which just reads an attachment mapping file where some UIs
 * get one or more attachments and models this exact attachment for the
 * {@link Tree} <tt>data</tt> given in the constructor.
 * </p>
 * <p>
 * This class is meant for extension for more sophisticated functions such as
 * inheritance, adding default values for UIs not given an attachment in the
 * mapping file etc.<br>
 * The general flow is the following:
 * <ol>
 * <li><tt>buildDescriptorAttachmentFromFile</tt> is called as an initial step
 * in which predefined attachments are read from file and given to the
 * respective tree vertices. The attachment are given to the vertices rather
 * than descriptors directly to allow for accounting for different paths between
 * descriptors. Since however the attachments are given for descriptors in this
 * step, all tree vertices of a descriptor obtain the same attachments by calls
 * to <tt>addAttachmentToDescriptor</tt>.
 * <ol>
 * <li>Sub-calls to <tt>addAttachmentToDescriptor</tt> get the attachment which
 * was read from file by default; can be overwritten to change this behavior.</li>
 * <li>After this, <tt>afterBuildTreeVertexAttachment</tt> is called once for
 * post-processing steps. Can be overwritten and does nothing by default.
 * </ol>
 * </li>
 * <li>You then call <tt>deriveDescriptorAttachment</tt> to get
 * descriptor-attachments from the tree-vertex-attachments created in the
 * initial step. By default, all attachments of the tree vertices of a
 * descriptor are united in a set-like manner, i.e. duplicates are eliminated.
 * In this phase, the following sub-calls happen:
 * <ol>
 * <li><tt>getDescriptorAttachment</tt> is called for all descriptors which have
 * no tree vertex with an attachment yet. Thus, you may add attachments to
 * particular descriptors directly (not tree vertices) which don't have got an
 * attachment in the initial phase. Can be overwritten, e.g. to give attachments
 * to descriptors which did not get one to begin with.</li>
 * <li><tt>cleanUpAttachments</tt> is called for <it>each</it> descriptor and
 * takes the list of all attachments for a particular descriptor given by all
 * its tree vertices. Here happens a unification by default, so that duplicate
 * attachments are eliminated. Can be overwritten to change this behavior.</li>
 * <li><tt>afterDeriveDescriptorAttachment</tt> is then called and offers a last
 * opportunity for post-processing. May be overwritten.
 * </ol>
 * </li>
 * </ol>
 * For main extension points (in terms of method overriding) refer to the links
 * below.
 * </p>
 * 
 * @see #addAttachmentToDescriptor(List, Descriptor)
 * @see #cleanUpAttachments(List)
 * @see #afterBuildDescriptorAttachment()
 * @see #afterDeriveDescriptorAttachments()
 * @see #getDescriptorAttachment(Descriptor)
 * @author faessler
 * 
 */

public class BaseAttacher {

	private static final Logger logger = LoggerFactory
			.getLogger(BaseAttacher.class);

	protected Tree data;
	protected ArrayListMultimap<TreeVertex, Attachment> treeVertexAttachments;
	protected ArrayListMultimap<Descriptor, Attachment> descriptorAttachments;

	private String name;

	public BaseAttacher(Tree data) {
		this.data = data;
	}

	/**
	 * <p>
	 * This is the initial attachment phase. The file
	 * <tt>categoryMapFilename</tt> is to contain a map from UI to one or
	 * multiple attachment values in the format<br/>
	 * <tt>UI&lt;tab&gt;attachmentValue1|attachmentValue2|...|attachmentValueN&lt;newline&gt;</tt>
	 * , for example
	 * <p>
	 * <samp> D019813 3|2<br/>
	 * D015632 3|2<br/>
	 * D015057 3<br/>
	 * </samp>
	 * </p>
	 * 
	 * @param categoryMapFilename
	 *            Name of a file containing attachments for descriptors.
	 * @throws IOException
	 */
	public void buildTreeVertexAttachmentFromFile(String categoryMapFilename)
			throws IOException {
		treeVertexAttachments = ArrayListMultimap.create();

		// Firstly, read the existing attachments from file.
		ArrayListMultimap<String, String> categoryMap = readCategoryMap(categoryMapFilename);

		// Then, build these directly defined categorizations.
		for (String ui : categoryMap.keySet()) {
			List<String> categories = categoryMap.get(ui);

			Descriptor descriptor = data.getDescriptorByUi(ui);

			// May happen e.g. due to shift to a new MeSH version.
			if (descriptor == null) {
				logger.warn("unknown DescriptorUI '{}'. I ignored it.", ui);
				continue;
			}

			addAttachmentToDescriptor(categories, descriptor);
		}
		afterBuildTreeVertexAttachment();
	}
	
	
	/**
	 * <p>
	 * <b>-- Overriding point --</b>
	 * </p>
	 * <p>
	 * This method is called by
	 * {@link BaseAttacher#buildTreeVertexAttachmentFromFile(String)} for each
	 * Descriptor UI in the mapping file. By overriding this method, you achieve
	 * the following:
	 * <ul>
	 * <li>Adding more specific <code>Attachment</code> objects to the
	 * attachments then just {@link Attachment}.</li>
	 * <li>Adding subsequent attachments which depend on the particular
	 * descriptor <code>descriptor</code>, e.g. for inheritance purposes.</li>
	 * <li>Whatever you additionally want to do for each descriptor in the
	 * mapping file.</li>
	 * </ul>
	 * </p>
	 * 
	 * @param categories
	 * @param descriptor
	 */
	protected void addAttachmentToDescriptor(List<String> categories,
			Descriptor descriptor) {
		for (TreeVertex vertex : descriptor.getTreeVertices()) {
			for (String category : categories){
				treeVertexAttachments.put(vertex, new Attachment(category));
			}
		}
	}

	/**
	 * <p>
	 * <b>-- Overriding point --</b>
	 * </p>
	 * <p>
	 * This method is called by
	 * {@link BaseAttacher#buildTreeVertexAttachmentFromFile(String)} after
	 * {@code #addAttachmentToDescriptor(List, Descriptor)} has been called for
	 * each descriptor in the mapping file.
	 * </p>
	 * <p>
	 * Here, you may perform some post-processing after each tree vertex has its
	 * attachments and before the descriptor attachments are derived.
	 * </p>
	 */
	protected void afterBuildTreeVertexAttachment() {
	};
	

	/**
	 * @param categoryMapFilename
	 * @return
	 * @throws IOException
	 */
	protected ArrayListMultimap<String, String> readCategoryMap(
			String categoryMapFilename) throws IOException {
		ArrayListMultimap<String, String> categoryMap = ArrayListMultimap
				.create();

		BufferedReader br = new BufferedReader(new FileReader(
				categoryMapFilename));
		String line;
		while ((line = br.readLine()) != null) {
			// Format: "UI<tab>cat1|cat2|cat3|..."
			String[] splits = line.trim().split("\\t");
			String ui = splits[0].trim();

			// "raw" because the String could contain un-trimmed white spaces.
			String[] rawCategories = splits[1].split("\\|");
			for (int i = 0; i < rawCategories.length; i++) {
				String category = rawCategories[i].trim();
				if (category != null && category.length() > 0)
					categoryMap.put(ui, category);
			}
		}
		return categoryMap;
	}
	
	
	
	/**
	 * <p>
	 * Creates the attachments per descriptor rather than per TreeVertex.
	 * </p>
	 * <p>
	 * All attachments of all TreeVertices associated with a descriptor are
	 * retrieved and cleaned. Cleaning means that eventually for each category
	 * only one attachment instance is given to the descriptor, viz. the one
	 * with shortest distance to the category origin. The descriptor attachments
	 * are sorted with regards to this distance.
	 * </p>
	 * 
	 * @param data
	 */
	public void deriveDescriptorAttachment() {
		if (treeVertexAttachments == null)
			throw new IllegalStateException(
					"You need to build the tree vertex attachments before deriving the descriptor attachments from them.");

		descriptorAttachments = ArrayListMultimap.create();

		Collection<Descriptor> descriptors = data.getAllDescriptors();
		logger.info("Deriving category attachments for {} descriptors.",
				descriptors.size());
		for (Descriptor descriptor : descriptors) {
			List<Attachment> attachmentsForVertices = new ArrayList<Attachment>();
			for (TreeVertex vertex : descriptor.getTreeVertices()) {
				attachmentsForVertices
						.addAll(treeVertexAttachments.get(vertex));
			}
			if (attachmentsForVertices.size() == 0) {
				attachmentsForVertices = getDescriptorAttachment(descriptor);
				if (attachmentsForVertices.size() == 0)
					logger.warn(
							"No category attachment for descriptor with UI '{}' and name '{}'.",
							descriptor.getUI(), descriptor.getName());
			}
			List<? extends Attachment> attachmentsForDescriptor = cleanUpAttachments(attachmentsForVertices);
			descriptorAttachments.putAll(descriptor, attachmentsForDescriptor);
		}
		afterDeriveDescriptorAttachments();
	}

	/**
	 * <p>
	 * <b>-- Overriding point --</b>
	 * </p>
	 * <p>
	 * This method is called by {@link #deriveDescriptorAttachment()} for each
	 * descriptor which did not obtain an attachment by
	 * {@link #addAttachmentToDescriptor(List, Descriptor)} during the
	 * {@link #buildTreeVertexAttachmentFromFile(String)} phase.
	 * <p>
	 * Here you may define default attachments for example.
	 * </p>
	 * 
	 * @param descriptor
	 *            Descriptor which does not have any attachments yet (although
	 *            all tree numbers should have gotten their attachments by now).
	 * @return
	 */
	protected List<Attachment> getDescriptorAttachment(Descriptor descriptor) {
		return descriptorAttachments.get(descriptor);
	}

	/**
	 * <p>
	 * <b>-- Overriding point --</b>
	 * </p>
	 * <p>
	 * This method is called by
	 * {@link BaseAttacher#deriveDescriptorAttachment()} after collecting all
	 * vertex attachments for a descriptor and before storing the list returned
	 * by this method as the descriptor's final attachments.
	 * </p>
	 * <p>
	 * By overriding this method you may clean up, for example remove
	 * duplicates, apply a particular order etc.
	 * </p>
	 * 
	 * @param attachmentsForVertices
	 *            The attachments for all tree vertices associated with a
	 *            particular descriptor.
	 * @return The clean-up list of attachments for the descriptor to whose tree
	 *         nodes the attachments in <code>attachmentsForVertices</code>
	 *         belong.
	 */
	protected List<? extends Attachment> cleanUpAttachments(
			List<Attachment> attachmentsForVertices) {
		return unifyAttachments(attachmentsForVertices);
	}

	/**
	 * <p>
	 * <b>-- Overriding point --</b>
	 * </p>
	 * <p>
	 * This method is called by
	 * {@link BaseAttacher#deriveDescriptorAttachment()} after all descriptor
	 * attachments have been created.
	 * </p>
	 * 
	 */
	protected void afterDeriveDescriptorAttachments() {
	}

	protected List<? extends Attachment> unifyAttachments(
			List<Attachment> attachments) {
		Set<Attachment> unifiedAttachments = new HashSet<Attachment>(
				attachments);
		return Lists.newArrayList(unifiedAttachments);
	}

	/**
	 * <p>
	 * The name set here will serve as header when storing the attachment as a
	 * file.
	 * </p>
	 * 
	 * @param name
	 *            Header of the column for this attachment.
	 */
	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void writeDescriptorAttachmentsToFile(String filename)
			throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(filename));
		logger.info(
				"Writing descriptor attachments for {} descriptors to '{}'.",
				descriptorAttachments.keySet().size(), filename);

		for (Descriptor descriptor : descriptorAttachments.keySet()) {
			String output = descriptor.getUI() + "\t";
			List<Attachment> attachments = descriptorAttachments
					.get(descriptor);
			output += StringUtils.join(attachments, "|");
			bw.write(output + "\n");
		}
		bw.close();
	}

	public static void writeDescriptorAttachmentsToFile(String filename,
			BaseAttacher... attachers) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(filename));
		List<Descriptor> descriptors = Lists
				.newArrayList(attachers[0].descriptorAttachments.keySet());

		logger.info(
				"Writing descriptor attachments for {} descriptors to '{}'.",
				attachers[0].descriptorAttachments.keySet().size(), filename);

		List<String> output = Lists.newArrayList("TermID(UI)");
		for (BaseAttacher attacher : attachers) {
			output.add(attacher.getName());
		}
		bw.write(StringUtils.join(output, "\t") + "\n");

		for (Descriptor descriptor : descriptors) {
			output = Lists.newArrayList(descriptor.getUI());
			for (BaseAttacher attacher : attachers) {
				List<Attachment> attachments = attacher.descriptorAttachments
						.get(descriptor);
				
					output.add(StringUtils.join(attachments, "|"));
			}
			bw.write(StringUtils.join(output, "\t") + "\n");
		}
		
		bw.close();
	}
}