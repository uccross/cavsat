package com.beans;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Query {
	private Set<Atom> atoms;
	private List<String> freeVars;

	public Query() {
		super();
		this.atoms = new HashSet<Atom>();
		this.freeVars = new ArrayList<String>();
	}

	public int getSize() {
		return this.atoms.size();
	}

	public boolean isBoolean() {
		return this.freeVars.size() == 0;
	}

	public Set<Atom> getAtoms() {
		return atoms;
	}

	public void setAtoms(Set<Atom> atoms) {
		this.atoms = atoms;
	}

	public void addAtom(Atom atom) {
		this.atoms.add(atom);
	}

	public List<String> getFreeVars() {
		return freeVars;
	}

	public String getFreeVarsCSV() {
		String output = "";
		for (String var : freeVars) {
			output += var + ",";
		}
		if (!output.isEmpty()) {
			output = output.substring(0, output.length() - 1);
		}
		return output;
	}

	public void setFreeVars(List<String> freeVars) {
		this.freeVars = freeVars;
	}

	public String getAtomNamesCSV() {
		String atomsCSV = "";
		for (Atom a : this.atoms) {
			atomsCSV += a.getName() + ",";
		}
		return atomsCSV.substring(0, atomsCSV.length() - 1);
	}

	public Set<String> getParticipatingRelationNames() {
		Set<String> relationNames = new HashSet<String>();
		for (Atom atom : this.atoms) {
			relationNames.add(atom.getName());
		}
		return relationNames;
	}

	public int getAtomsCountByName(String name) {
		int count = 0;
		for (Atom a : getAtoms()) {
			if (a.getName().equals(name))
				count++;
		}
		return count;
	}

	/**
	 * @param atomName
	 * @return First atom of the matching name, otherwise null
	 */
	public Atom getAtomByName(String atomName) {
		for (Atom atom : this.atoms) {
			if (atom.getName().equalsIgnoreCase(atomName))
				return atom;
		}
		return null;
	}

	public void print() {
		System.out.println("Query:");
		for (Atom a : getAtoms()) {
			System.out.println(a.getName() + "(" + a.getAttributesCSV() + ")" + "\t keys: " + a.getKeyAttributesCSV());
		}
		System.out.println("Free variables:");
		System.out.println(this.freeVars);
		System.out.println("-------------------------------------------------------------");
	}
}
