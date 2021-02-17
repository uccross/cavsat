/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.bean;

import java.util.HashSet;
import java.util.Set;

import lombok.Data;

@Data
public class Schema {
	Set<Relation> relations;
	Set<String> denialConstraints;

	public Schema() {
		super();
		this.relations = new HashSet<Relation>();
		this.denialConstraints = new HashSet<String>();
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
		for (String constraint : this.denialConstraints) {
			System.out.println(constraint);
		}
		System.out.println("-------------------------------------------------------------");
	}
}
