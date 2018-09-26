package com.beans;

import java.util.ArrayList;
import java.util.List;

public class Atom {
	private String name;
	private int atomIndex;
	private List<String> attributes;

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

	public String getAttributesCSV() {
		String result = "";
		for (String attribute : getAttributes()) {
			result += attribute + ",";
		}
		return result.substring(0, result.length() - 1);
	}
}
