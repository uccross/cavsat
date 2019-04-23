/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

import java.util.HashSet;
import java.util.Set;

public class Dependency {
	private Set<Integer> left;
	private Set<Integer> right;

	public Dependency() {
		super();
		this.left = new HashSet<Integer>();
		this.right = new HashSet<Integer>();
	}

	public Set<Integer> getLeft() {
		return left;
	}

	public void setLeft(Set<Integer> left) {
		this.left = left;
	}

	public Set<Integer> getRight() {
		return right;
	}

	public void setRight(Set<Integer> right) {
		this.right = right;
	}

	public void addToLeft(int attributeIndex) {
		this.left.add(attributeIndex);
	}

	public void addToRight(int attributeIndex) {
		this.right.add(attributeIndex);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		if(this.equals(null))
			return null;
		String result = "";
		for (int i : left) {
			result += i + ",";
		}
		result = result.substring(0, result.length() - 1) + "->";
		for (int i : right) {
			result += i + ",";
		}
		result = result.substring(0, result.length() - 1);
		return result;
	}

}
