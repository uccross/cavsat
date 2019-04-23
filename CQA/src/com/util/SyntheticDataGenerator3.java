/*******************************************************************************
 * Copyright 2019 Regents of the University of California. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be found in the LICENSE.txt file at the root of the project.
 ******************************************************************************/
package com.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.beans.Atom;
import com.beans.Query;
import com.beans.Relation;
import com.beans.Schema;

public class SyntheticDataGenerator3 {
	private List<String> thirdColumnValues;

	public static void main(String[] args) throws SQLException {
		List<Query> uCQ = new ProblemParser().parseUCQ(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\toyquery1.txt");
		Schema schema = new ProblemParser().parseSchema(
				"C:\\Users\\Akhil\\OneDrive - ucsc.edu\\Abhyas\\CQA\\MaxHS-3.0\\build\\release\\bin\\toyschema1.txt");
		Connection con = new DBEnvironment().getConnection();
		SyntheticDataGenerator3 gen = new SyntheticDataGenerator3();
		gen.generateThirdColumnValues(100000);
		gen.generateConsistent(con, uCQ.get(0), 975000, 0.2, true);
		gen.addInconsistency(con, schema, uCQ.get(0), 50000, 2);
		gen.adjustFactIDs(con, uCQ.get(0));
	}

	public SyntheticDataGenerator3() {
		super();
		this.thirdColumnValues = new ArrayList<String>();
	}

	public void generateThirdColumnValues(int n) {
		RandomString gen = new RandomString(5, ThreadLocalRandom.current());
		while (thirdColumnValues.size() < n) {
			thirdColumnValues.add(gen.nextString());
		}
	}

	public int adjustFactIDs(Connection con, Query q) throws SQLException {
		int offset = 0;
		for (Atom atom : q.getAtoms()) {
			offset += con.prepareStatement("UPDATE " + atom.getName() + " SET FactID = FactID + " + offset)
					.executeUpdate();
		}
		return offset;
	}

	public void addInconsistency(Connection con, Schema schema, Query q, int n, int keyGroupSize) throws SQLException {
		int f = n / keyGroupSize;
		RandomString gen = new RandomString(5, ThreadLocalRandom.current());
		for (Relation r : schema.getRelationsByNames(q.getParticipatingRelationNames())) {
			String selectQuery = "SELECT ", prefix = "";
			for (String keyAttr : r.getKeyAttributesList()) {
				selectQuery += prefix + keyAttr;
				prefix = ", ";
			}
			selectQuery += " FROM " + r.getName() + " ORDER BY RANDOM() LIMIT " + f;
			ResultSet rs = con.prepareStatement(selectQuery).executeQuery();
			PreparedStatement psInsert = getInsertStatement(r, con);
			while (rs.next()) {
				int position = 0, keyPosition = 0;
				for (int j = 0; j < keyGroupSize - 1; j++) {
					for (position = 1; position < r.getNoOfAttributes(); position++) {
						String attr = r.getAttributes().get(position - 1);
						keyPosition = r.getKeyAttributesList().indexOf(attr);
						if (keyPosition == -1) {
							psInsert.setString(position, gen.nextString());
						} else {
							psInsert.setString(position, rs.getString(attr));
						}
					}
					String attr = r.getAttributes().get(position - 1);
					keyPosition = r.getKeyAttributesList().indexOf(attr);
					if (keyPosition == -1) {
						psInsert.setString(position,
								thirdColumnValues.get((int) (Math.random() * thirdColumnValues.size())));
					} else {
						psInsert.setString(position, rs.getString(attr));
					}
					psInsert.addBatch();
				}
			}
			rs.close();
			psInsert.executeBatch();
		}
	}

	public void generateConsistent(Connection con, Query q, int n, double w, boolean includeFactID)
			throws SQLException {
		List<PreparedStatement> psInserts = new ArrayList<PreparedStatement>();
		for (Atom atom : q.getAtoms()) {
			createRelation(atom, con, includeFactID);
			psInserts.add(getInsertStatement(atom, con));
		}
		RandomString gen = new RandomString(5, ThreadLocalRandom.current());
		Random rand = new Random();
		List<String> vars = q.getAllVars();
		int i = 0, k = 0;
		while (i < n) {
			if (rand.nextDouble() < w) {
				String[] tTuple = new String[vars.size()];
				for (int j = 0; j < tTuple.length; j++)
					tTuple[j] = gen.nextString();
				for (int atomIndex = 0; atomIndex < q.getAtoms().size(); atomIndex++) {
					for (k = 1; k <= q.getAtoms().get(atomIndex).getVars().size(); k++) {
						String var = q.getAtoms().get(atomIndex).getVars().get(k - 1);
						psInserts.get(atomIndex).setString(k, tTuple[vars.indexOf(var)]);
					}
					if (k == 4)
						psInserts.get(atomIndex).setString(3,
								thirdColumnValues.get((int) (Math.random() * thirdColumnValues.size())));
					psInserts.get(atomIndex).addBatch();
				}
			} else {
				int varIndex = 0;
				for (int atomIndex = 0; atomIndex < q.getAtoms().size(); atomIndex++) {
					for (varIndex = 1; varIndex < q.getAtoms().get(atomIndex).getVars().size(); varIndex++) {
						psInserts.get(atomIndex).setString(varIndex, gen.nextString());
					}
					psInserts.get(atomIndex).setString(varIndex,
							thirdColumnValues.get((int) (Math.random() * thirdColumnValues.size())));
					psInserts.get(atomIndex).addBatch();
				}
			}
			i++;
		}

		for (PreparedStatement psInsert : psInserts)
			psInsert.executeBatch();
	}

	private void createRelation(Atom atom, Connection con, boolean includeFactID) throws SQLException {
		con.prepareStatement("DROP TABLE IF EXISTS " + atom.getName()).execute();
		String createQuery = "CREATE TABLE " + atom.getName() + " (";
		String prefix = "";
		for (int i = 0; i < atom.getVars().size(); i++) {
			createQuery = createQuery + prefix + "COLUMN" + i + " TEXT";
			prefix = ",";
		}
		if (includeFactID)
			createQuery += ", FactID SERIAL";
		createQuery += ")";
		con.prepareStatement(createQuery).execute();
	}

	private PreparedStatement getInsertStatement(Atom atom, Connection con) throws SQLException {
		String insertQuery = "INSERT INTO " + atom.getName() + " VALUES(";
		String prefix = "";
		for (int i = 0; i < atom.getVars().size(); i++) {
			insertQuery = insertQuery + prefix + "?";
			prefix = ",";
		}
		insertQuery += ")";
		return con.prepareStatement(insertQuery);
	}

	private PreparedStatement getInsertStatement(Relation r, Connection con) throws SQLException {
		String insertQuery = "INSERT INTO " + r.getName() + " VALUES(";
		String prefix = "";
		for (int i = 0; i < r.getNoOfAttributes(); i++) {
			insertQuery = insertQuery + prefix + "?";
			prefix = ",";
		}
		insertQuery += ")";
		return con.prepareStatement(insertQuery);
	}
}
