package com.beans;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Query {
	private List<Atom> atoms;
	private List<String> freeVars;

	public Query() {
		super();
		this.atoms = new ArrayList<Atom>();
		this.freeVars = new ArrayList<String>();
	}

	public int getSize() {
		return this.atoms.size();
	}

	public boolean isBoolean() {
		for (String var : this.freeVars) {
			if (!var.isEmpty()) {
				return false;
			}
		}
		return true;
	}

	public List<Atom> getAtoms() {
		return atoms;
	}

	public void setAtoms(List<Atom> atoms) {
		this.atoms = atoms;
	}

	public void addAtom(Atom atom) {
		this.atoms.add(atom);
	}

	public List<String> getFreeVars() {
		return freeVars;
	}

	public Atom getAtomByPosition(int index) {
		return this.atoms.get(index);
	}

	public int getPositionOfAtom(Atom atom) {
		return this.atoms.indexOf(atom);
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

	public List<String> getAllVars() {
		List<String> allVars = new ArrayList<String>();
		for (Atom atom : this.atoms) {
			for (String var : atom.getVars()) {
				if (!allVars.contains(var))
					allVars.add(var);
			}
		}
		return allVars;
	}

	public String getVarByPosition(int index) {
		return getAllVars().get(index);
	}

	public int getPositionOfVar(String var) {
		return getAllVars().indexOf(var);
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
			System.out.println(a.getName() + "(" + a.getVarsCSV() + ")" + "\t keys: " + a.getKeyVars() + "\t non-keys: "
					+ a.getNonKeyVars());
		}
		System.out.println("Free variables:");
		System.out.println(this.freeVars);
		System.out.println("-------------------------------------------------------------");
	}
}
