package com.beans;

import java.util.ArrayList;
import java.util.List;

public class Atom {
	private String name;
	private int atomIndex;
	private List<String> attributes;
	private List<String> keyAttributes;

	@Override
	public boolean equals(Object arg0) {
		// TODO Auto-generated method stub
		if (!(arg0 instanceof Atom)) {
			return false;
		} else {
			return ((Atom) arg0).getName().equals(this.name) && ((Atom) arg0).getAttributes().equals(this.attributes);
		}
	}

	@Override
	public int hashCode() {
		return 0;
	}

	public Atom(String name) {
		super();
		this.name = name;
		this.attributes = new ArrayList<String>();
		this.keyAttributes = new ArrayList<String>();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAtomIndex() {
		return atomIndex;
	}

	public void setAtomIndex(int atomIndex) {
		this.atomIndex = atomIndex;
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<String> attributes) {
		this.attributes = attributes;
	}

	public void addAttribute(String attribute) {
		this.attributes.add(attribute);
	}

	public List<String> getKeyAttributes() {
		return keyAttributes;
	}

	public void setKeyAttributes(List<String> keyAttributes) {
		this.keyAttributes = keyAttributes;
	}

	public void addKeyAttribute(String attribute) {
		this.keyAttributes.add(attribute);
	}

	public String getAttributeByIndex(int index) {
		return this.attributes.get(index - 1); // List indexes start from 0
	}

	public String getAttributesCSV() {
		String result = "";
		for (String attribute : getAttributes()) {
			result += attribute + ",";
		}
		if (!result.isEmpty())
			result = result.substring(0, result.length() - 1);
		return result;
	}

	public String getKeyAttributesCSV() {
		String result = "";
		for (String attribute : getKeyAttributes()) {
			result += attribute + ",";
		}
		if (!result.isEmpty())
			result = result.substring(0, result.length() - 1);
		return result;
	}

	public String getNameWithAttributes() {
		return this.name + "(" + getAttributesCSV() + ")";
	}

	public List<String> getSharedVars(Atom atom) {
		List<String> sharedVars = new ArrayList<String>();
		for (String var : this.attributes) {
			if (atom.getAttributes().contains(var))
				sharedVars.add(var);
		}
		return sharedVars;
	}
}
