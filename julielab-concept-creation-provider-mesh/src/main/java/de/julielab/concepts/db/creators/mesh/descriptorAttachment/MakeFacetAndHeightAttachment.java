/**
 * MakeFacetAndHeightAttachment.java
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
 * Creation date: 15.03.2012
 **/

/**
 * 
 */
package de.julielab.concepts.db.creators.mesh.descriptorAttachment;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.exchange.Parser4Mesh;

/**
 * <p>
 * This class outputs for the read MeSH descriptors a "facet" and a "height"
 * label.
 * </p>
 * <p>
 * Facet labels are just derived from the MeSH top categories, such as Anatomy
 * [A], Organisms [B], Diseases [C] etc. The shortcuts A, B, C etc. are output
 * as a label.
 * </p>
 * <p>
 * The output height for a descriptor d is the minimum height over all tree
 * vertices associated with d.
 * </p>
 * <p>
 * This class does actually not employ any attachment classes as the desired
 * information is contained in the read (MeSH) tree anyway.
 * </p>
 * 
 * @author faessler
 * 
 */
public class MakeFacetAndHeightAttachment {
	public static void main(String[] args) {
		// local variables
		Tree data = new Tree("myTree");
		String meshXmlInFilePath = "";
		String outFilePath = "";
		String meshXmlDtdFilePath = "";

		// check command line arguments
		System.out.println("# Checking command line arguments ... ");
		String usage = "expected arguments are in this order: \n 'Path to full MeSH XML file' \n "
				+ "'Path to MeSH XML dtd file' \n 'Base path to output file (endings will automatically be added)'";
		if (args.length == 3) {
			meshXmlInFilePath = args[0];
			meshXmlDtdFilePath = args[1];
			outFilePath = args[2];
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

		writeRootAndHeightAttachmet(data, outFilePath);

		// print some information
		data.printInfo(System.out);
	}

	/**
	 * @param data
	 * @param outFilePath 
	 */
	private static void writeRootAndHeightAttachmet(Tree data, String outFilePath) {
		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new FileWriter(outFilePath + ".tsv"));
			for (Descriptor descriptor : data.getAllDescriptors()) {
				// There is always one descriptor without UI - root for JGraphT or something?
				if (descriptor.getUI() == null)
					continue;
				
				TreeVertex bestTreeVertex = data.getBestTreeVertexOf(descriptor);
				
				char facet = data.treeNumberOf(bestTreeVertex).getFirstPartialNumber().charAt(0);
				int height = data.heightOf(bestTreeVertex);
				
				String facetLabel = "facet_" + facet;
				String heightLabel = "height_" + height;
				String ui = descriptor.getUI();
				
				bw.write(ui + "\t" + facetLabel + "|" + heightLabel + "\n");
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
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
