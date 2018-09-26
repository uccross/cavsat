package com.beans;

import java.util.HashSet;
import java.util.Set;

public class Schema {
	Set<Relation> relations;

	public Schema() {
		super();
		this.relations = new HashSet<Relation>();
	}

	public Set<Relation> getRelations() {
		return relations;
	}

	public void setRelations(Set<Relation> relations) {
		this.relations = relations;
	}

	public String getAttributeNameByIndex(String relationName, int index) {
		for (Relation r : this.relations) {
			if (r.getName().equals(relationName)) {
				return r.getAttributes().get(index - 1); // List indexes start from 0, database indexes start from 1.
			}
		}
		return null;
	}

	public Relation getRelationByName(String relationName) {
		for (Relation relation : this.relations) {
			if (relation.getName().equalsIgnoreCase(relationName)) {
				return relation;
			}
		}
		return null;
	}

	public Set<Relation> getRelationsByNames(Set<String> relationNames) {
		Set<Relation> relations = new HashSet<Relation>();
		for (String name : relationNames) {
			for (Relation r : this.relations) {
				if (r.getName().equalsIgnoreCase(name)) {
					relations.add(r);
				}
			}
		}
		return relations;
	}

	public void print() {
		System.out.println("Schema:");
		for (Relation r : this.relations) {
			r.print();
		}
		System.out.println("-------------------------------------------------------------");
	}
}