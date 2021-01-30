/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.bean;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Relation {
	private String name;
	private List<String> attributes;
	private List<String> types;
	private Set<Integer> keyAttributes;
	private Dependency dependency;
	private Set<String> relevantAttributes;

	@Override
	public boolean equals(Object arg0) {
		// TODO Auto-generated method stub
		if (!(arg0 instanceof Relation))
			return false;
		else
			return ((Relation) arg0).getName().equals(this.name)
					&& ((Relation) arg0).getAttributes().equals(this.attributes);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Relation(String name) {
		super();
		this.name = name;
		this.keyAttributes = new HashSet<Integer>();
		this.attributes = new ArrayList<String>();
		this.types = new ArrayList<String>();
		this.dependency = new Dependency();
		this.relevantAttributes = new HashSet<String>();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getNoOfAttributes() {
		return this.attributes.size();
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<String> attributes) {
		this.attributes = attributes;
	}
	
	public List<String> getTypes() {
		return types;
	}

	public void setTypes(List<String> types) {
		this.types = types;
	}

	public Set<Integer> getKeyAttributes() {
		return keyAttributes;
	}

	public List<String> getKeyAttributesList() {
		List<String> list = new ArrayList<String>();
		for (int key : keyAttributes) {
			list.add(attributes.get(key - 1));
		}
		return list;
	}
	
	public Dependency getDependency() {
		return dependency;
	}

	public void setDependency(Dependency dependency) {
		this.dependency = dependency;
	}

	public void setKeyAttributes(Set<Integer> keyAttributes) {
		this.keyAttributes = keyAttributes;
	}

	public void addKeyAttribute(int attributeIndex) {
		this.keyAttributes.add(attributeIndex);
	}

	public void addAttribute(String attributeName) {
		this.attributes.add(attributeName);
	}

	public Set<String> getRelevantAttributes() {
		return relevantAttributes;
	}

	public void setRelevantAttributes(Set<String> relevantAttributes) {
		this.relevantAttributes = relevantAttributes;
	}

	public String getKeysCSV() {
		String output = "";
		for (int i : this.keyAttributes) {
			output = output + i + ",";
		}
		if (output.isEmpty())
			return null;
		return output.substring(0, output.length() - 1);
	}

	public String getAllAttributesCSV(String prefix) {
		String output = "";
		if (null != prefix && !prefix.isEmpty())
			prefix += ".";
		for (String attributeName : getAttributes()) {
			output = output + prefix + attributeName + ",";
		}
		if (output.isEmpty())
			return null;
		return output.substring(0, output.length() - 1);
	}

	public String getAttributesFromIndexesCSV(Set<Integer> indexes, String prefix) {
		String output = "";
		if (prefix != null && !prefix.isEmpty())
			prefix += ".";
		else
			prefix = "";
		for (int i : indexes) {
			output = output + prefix + this.attributes.get(i - 1) + ","; // List index starts from 0, attribute
																			// indexes start from 1
		}
		if (output.isEmpty())
			return null;
		return output.substring(0, output.length() - 1);
	}

	public String getRelevantAttributesCSV() {
		String output = "";
		for (String attribute : this.relevantAttributes) {
			output += attribute + ",";
		}
		if (output.isEmpty())
			return null;
		return output.substring(0, output.length() - 1);
	}

	public String getAttributeIndexesCSV() {
		String output = "";
		for (int i = 1; i <= getNoOfAttributes(); i++) {
			output = output + i + ",";
		}
		if (output.isEmpty())
			return null;
		return output.substring(0, output.length() - 1);
	}

	public void print() {
		System.out.println(getName() + "(" + getAllAttributesCSV("") + ")");
		System.out.println("Key column indexes: " + getKeysCSV());
		System.out.println("Relevant attributes: " + getRelevantAttributes());
	}
}
