/*******************************************************************************
 * Copyright 2020 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/

package edu.cavsat.model.bean;

import java.util.HashSet;
import java.util.Set;

public class Schema {
	Set<Relation> relations;
	Set<DenialConstraint> constraints;

	public Schema() {
		super();
		this.relations = new HashSet<Relation>();
		this.constraints = new HashSet<DenialConstraint>();
	}

	public Set<Relation> getRelations() {
		return relations;
	}

	public void setRelations(Set<Relation> relations) {
		this.relations = relations;
	}

	public Set<DenialConstraint> getConstraints() {
		return constraints;
	}

	public void setConstraints(Set<DenialConstraint> constraints) {
		this.constraints = constraints;
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
		for (DenialConstraint constraint : this.constraints) {
			System.out.println(constraint);
		}
		System.out.println("-------------------------------------------------------------");
	}
}
