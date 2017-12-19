/**
 * ProcessAttachment.java
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
 * Creation date: 12.03.2012
 **/

/**
 * 
 */
package de.julielab.semedico.mesh.descriptorAttachment;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import de.julielab.semedico.mesh.Tree;
import de.julielab.semedico.mesh.TreeFilter;
import de.julielab.semedico.mesh.components.Descriptor;
import de.julielab.semedico.mesh.components.TreeVertex;
import de.julielab.semedico.mesh.exchange.Parser4Mesh;

/**
 * <p>
 * This class takes a version of the MeSH, a "white list" of UI
 * 
 * @author faessler
 * 
 */
public class MakeCategoryAttachment {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// local variables
		Tree data = new Tree("myTree");
		String meshXmlInFilePath = "";
		String outFilePath = "";
		String meshXmlDtdFilePath = "";
		String agingMeshUIWhiteListFilePath = "";
		String agingMeshAdditionalTermsFilePath = "";
		String ageingMeshCategoryMapping = "";
		String ageingMeshTermClassMapping = "";
		String agingWordlist = "";

		// check command line arguments
		System.out.println("# Checking command line arguments ... ");
		String usage = "expected arguments are in this order: \n 'Path to full MeSH XML file' \n "
				+ "'Path to MeSH XML dtd file' \n 'Base path to output file (endings will automatically be added)' "
				+ "\n 'Path to aging whitelist file' \n 'Path to MeSH XML additional aging descriptors' \n 'Path to UI-Category-Mapping' \n 'Path to UI-Ageing-Term-Class-Mapping' \n 'Path to Ageing-Wordlist'";
		if (args.length == 8) {
			meshXmlInFilePath = args[0];
			meshXmlDtdFilePath = args[1];
			outFilePath = args[2];
			agingMeshUIWhiteListFilePath = args[3];
			agingMeshAdditionalTermsFilePath = args[4];
			ageingMeshCategoryMapping = args[5];
			ageingMeshTermClassMapping = args[6];
			agingWordlist = args[7];
		} else {
			System.out.println("ERROR: " + usage);
			return;
		}
		System.out.println("# done\n");

		// import MeSH XML data
		System.out
				.println("# Importing descriptor records from MeSH XML file '"
						+ meshXmlInFilePath + "' ... ");
		fromMeshXmlWithSax(meshXmlInFilePath, meshXmlDtdFilePath, data);
		System.out.println("# done\n");

		// for Aging MeSH: add some new descriptors
		System.out
				.println("# Importing additional descriptor records for aging from XML file '"
						+ agingMeshAdditionalTermsFilePath + "' ... ");
		fromMeshXmlWithSax(agingMeshAdditionalTermsFilePath,
				meshXmlDtdFilePath, data);
		System.out.println("# done\n");

		// for Aging MeSH: filter data
		System.out.println("# Filter data using whitelist '"
				+ agingMeshUIWhiteListFilePath + "' ... ");
		TreeFilter filter = new TreeFilter(data, TreeFilter.THROW, TreeFilter.THROW);
		try {
			try (BufferedReader br = Files.newBufferedReader(Paths.get(agingMeshUIWhiteListFilePath))) {
 			filter.maskDescByUiList(br.lines().collect(Collectors.toList()), true, true); // true: apply
			}
//			filter.maskDescByUiList(IOUtils.readLines(new FileInputStream(
//					agingMeshUIWhiteListFilePath)), true, true); // true: apply
			// it
			// recursively

			// filter.keepFacets();
			filter.apply();
			// filter = new Filter(data, true);
			// filter.throwEmptyFacets();
			// filter.apply();
			System.out.println("# done\n");
			System.out.println("Hier ist die Groesse: " + data.getAllDescriptors().size());
			try {
				// for Aging MeSH category attachment: read already known ID
				// attachments
				RecursiveAttacher categoryAttacher = new RecursiveAttacher(data);
				categoryAttacher.setName("Categories");
				categoryAttacher
						.buildTreeVertexAttachmentFromFile(ageingMeshCategoryMapping);
				categoryAttacher.deriveDescriptorAttachment();

				// for Aging MeSH default child attachment: Read the class type
				// map
				// and attach the child-class (4) to every descriptor which has
				// no
				// definition in the mapping file (and thus, has not been
				// selected
				// manually by Rolf Huehne but is a descendant of one of them).
				DefaultAttacher defaultAttacher = new DefaultAttacher(data,
						new Attachment("4"));
				defaultAttacher.setName("Term_Class");
				defaultAttacher
						.buildTreeVertexAttachmentFromFile(ageingMeshTermClassMapping);
				defaultAttacher.deriveDescriptorAttachment();

				// search and add wordlist to file
				ScopenoteAttacher scopenoteAttacher = new ScopenoteAttacher(
						data);
				scopenoteAttacher.setName("WordList");
				scopenoteAttacher
						.buildTreeVertexAttachmentForScopenote(agingWordlist);
				scopenoteAttacher.deriveDescriptorAttachment();

				// BaseAttacher.writeDescriptorAttachmentsToFile(outFilePath,
				// categoryAttacher, defaultAttacher, scopenoteAttacher);
				buildRolfHuehneOutputErikVersion(categoryAttacher,
						defaultAttacher, scopenoteAttacher, data, outFilePath);
			} catch (IOException e) {
				e.printStackTrace();
			}

			// print some information
			data.printInfo(System.out);

		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * New version because Rolf Huehne did eventually decide not to only want
	 * the information about the original ancestor but about all descriptors
	 * from the original ancestor to the currently viewed descriptor.
	 * 
	 * @param categoryAttacher
	 * @param defaultAttacher
	 * @param scopenoteAttacher
	 * @param data
	 * @param filename
	 * @throws IOException
	 */
	private static void buildRolfHuehneOutputErikVersion(
			RecursiveAttacher categoryAttacher,
			DefaultAttacher defaultAttacher,
			ScopenoteAttacher scopenoteAttacher, Tree data, String filename)
			throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(filename));

		bw.write("TermID(UI) \tTermClass \tADistance \tATermID \t"
				+ "ATermClass \tCategory \tDisease \tWordlist");
		bw.newLine();

		Set<Descriptor> ageingDescs = new HashSet<Descriptor>();
		for (Descriptor d : categoryAttacher.descriptorAttachments.keys())
			ageingDescs.add(d);

		/*
		 * there are some descriptors that are not unique and due to time
		 * pressure I had to filter the results using this (quick and dirty)
		 * method the problem is somewhere within the base-attacher for it seems
		 * only descriptors with category 4 have copies
		 * 
		 * modersohn
		 */
		Set<Descriptor> descList = new HashSet<Descriptor>();

		for (Descriptor d : categoryAttacher.descriptorAttachments.keys()) {
			if (!descList.contains(d)) {
				descList.add(d);
				String[] o = new String[8];

				String currentDescUi = d.getUI();
				o[0] = currentDescUi;

				List<Attachment> catAtts = categoryAttacher
						.getDescriptorAttachment(d);
				List<Attachment> typeAtts = defaultAttacher
						.getDescriptorAttachment(d);
				List<Attachment> scopenotes = scopenoteAttacher
						.getDescriptorAttachment(d);

				o[7] = scopenotes.size() > 0 ? scopenotes.get(0)
						.getAttachment() : "";

				for (Attachment typeAtt : typeAtts) {
					o[1] = typeAtt.getAttachment();

					for (Attachment catAtt : catAtts) {
						RecursiveAttachment recAtt = (RecursiveAttachment) catAtt;
						for (TreeVertex ancestorVertex : recAtt
								.getPathFromAttachmentOrigin()) {
							String ancestorUi = ancestorVertex.getDescUi();
							Descriptor ancestor = data
									.getDescriptorByUi(ancestorUi);
							int ancestorDistance = recAtt.getDistance();
							o[2] = String.valueOf(ancestorDistance);
							String[] catAndDisease = recAtt.getCategory()
									.split("/");
							o[5] = catAndDisease[0];
							o[6] = catAndDisease.length == 2 ? catAndDisease[1]
									: "";
							o[3] = "";
							o[4] = "";
							if (!currentDescUi.equals(ancestorUi)) {
								o[3] = ancestor.getUI();

								for (Attachment ancestorTypeAtt : defaultAttacher
										.getDescriptorAttachment(ancestor)) {
									o[4] = ancestorTypeAtt.getAttachment();
									bw.write(StringUtils.join(o, "\t") + "\n");
								}
							} else if (recAtt.getPathFromAttachmentOrigin()
									.size() == 1) {
								bw.write(StringUtils.join(o, "\t") + "\n");
							}
						}
					}
				}
				if (scopenotes.size() > 1)
					throw new IllegalStateException(
							"More than one ageing-words-attachment: "
									+ Arrays.toString(o));

			}
		}
		bw.close();
	}

	private static void buildRolfHuehneOutput(
			RecursiveAttacher categoryAttacher,
			DefaultAttacher defaultAttacher,
			ScopenoteAttacher scopenoteAttacher, Tree data, String filename)
			throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(filename));

		bw.write("TermID(UI) \tTermClass \tADistance \tATermID \t"
				+ "ATermClass \tCategory \tDisease \tWordlist");
		bw.newLine();

		/*
		 * there are some descriptors that are not unique and due to time
		 * pressure I had to filter the results using this (quick and dirty)
		 * method the problem is somewhere within the base-attacher for it seems
		 * only descriptors with category 4 have copies
		 * 
		 * modersohn
		 */
		List<Descriptor> descList = new ArrayList<Descriptor>();

		for (Descriptor d : categoryAttacher.descriptorAttachments.keys()) {
			if (!descList.contains(d)) {
				descList.add(d);

				List<Attachment> catAtts = categoryAttacher
						.getDescriptorAttachment(d);
				List<Attachment> typeAtts = defaultAttacher
						.getDescriptorAttachment(d);
				List<Attachment> scopenotes = scopenoteAttacher
						.getDescriptorAttachment(d);

				for (Attachment typeAtt : typeAtts) {

					for (Attachment catAtt : catAtts) {

						RecursiveAttachment rec = (RecursiveAttachment) catAtt;

						if (0 == rec.getDistance()) {
							String[] s = rec.getCategory().split("/");

							for (Attachment scope : scopenotes) {

								bw.write(d.getUI() + "\t"
										+ typeAtt.getAttachment() + "\t"
										+ rec.getDistance() + "\t\t\t");
								if (1 < s.length) {
									bw.write(s[0] + "\t" + s[1] + "\t"
											+ scope.getAttachment());
								} else {
									bw.write(s[0] + "\t" + "\t"
											+ scope.getAttachment());
								}
								bw.newLine();
							}

						} else {
							TreeVertex origin = rec
									.getPathFromAttachmentOrigin().get(0);
							Descriptor ancestor = data
									.getDescriptorByVertex(origin);
							List<Attachment> ancestorClasses = defaultAttacher
									.getDescriptorAttachment(ancestor);

							for (Attachment att : catAtts) {

								RecursiveAttachment recCatAtt = (RecursiveAttachment) att;

								String[] s = recCatAtt.getCategory().split("/");

								for (Attachment ancClassAtt : ancestorClasses) {

									for (Attachment scope : scopenotes) {

										bw.write(d.getUI() + "\t"
												+ typeAtt.getAttachment()
												+ "\t" + rec.getDistance()
												+ "\t" + ancestor.getUI()
												+ "\t"
												+ ancClassAtt.getAttachment()
												+ "\t");

										if (1 < s.length) {
											bw.write(s[0] + "\t" + s[1] + "\t"
													+ scope.getAttachment());
										} else {
											bw.write(s[0] + "\t" + "\t"
													+ scope.getAttachment());
										}
										bw.newLine();
									}
								}
							}
						}
					}
				}
			}
		}
		bw.close();
	}

	private static void fromMeshXmlWithSax(String xmlFilepath,
			String dtdFilepath, Tree data) {
		try {
			XMLReader xmlReader = XMLReaderFactory.createXMLReader();

			// seems slower: BufferedReader reader = new BufferedReader(new
			// FileReader(xmlFilepath));
			FileReader reader = new FileReader(xmlFilepath);
			InputSource inputSource = new InputSource(reader);

			// TODO: create dtd file or simliar -> look at evernote note for xml
			// structure
			inputSource.setSystemId(dtdFilepath);

			Parser4Mesh saxHandler = new Parser4Mesh(data);
			xmlReader.setContentHandler(saxHandler);

			xmlReader.parse(inputSource);

		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
		} catch (IOException e) {
			System.err.println(e.getMessage());
		} catch (SAXException e) {
			System.err.println(e.getMessage());
		}
	}

}
