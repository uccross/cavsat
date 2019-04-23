/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.beans;

import java.util.ArrayList;
import java.util.List;

public class AtomFD {
	private List<String> left;
	private List<String> right;
	private String atomName;

	public AtomFD(String atomName) {
		super();
		this.atomName = atomName;
	}

	public AtomFD(String atomName, List<String> left, List<String> right) {
		super();
		this.left = new ArrayList<String>();
		for (String s : left) {
			if (!isConstant(s))
				this.left.add(s);
		}
		this.right = new ArrayList<String>();
		for (String s : right) {
			if (!isConstant(s))
				this.right.add(s);
		}
		this.atomName = atomName;
	}

	public List<String> getLeft() {
		return left;
	}

	public void setLeft(List<String> left) {
		this.left = left;
	}

	public List<String> getRight() {
		return right;
	}

	public void setRight(List<String> right) {
		this.right = right;
	}

	public String getAtomName() {
		return atomName;
	}

	public void setAtomName(String atomName) {
		this.atomName = atomName;
	}

	public void print() {
		String fd = this.atomName + "\t{";
		for (String attribute : this.left) {
			fd += attribute + ",";
		}
		if (fd.endsWith(","))
			fd = fd.substring(0, fd.length() - 1);
		fd += "} --> {";
		for (String attribute : this.right) {
			fd += attribute + ",";
		}
		if (fd.endsWith(","))
			fd = fd.substring(0, fd.length() - 1);
		fd += "}";
		System.out.println(fd);
	}

	private boolean isConstant(String attribute) {
		return attribute.startsWith("'") && attribute.endsWith("'");
	}
}
