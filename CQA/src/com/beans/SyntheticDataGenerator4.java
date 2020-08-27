package com.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.HashMap;
import java.util.HashSet;

import com.beans.Atom;
import com.beans.SQLQuery;
import com.beans.Relation;
import com.beans.Schema;


public class SyntheticDataGenerator4 {
	int setSize = 60000;		//size of generated HashMap tupleMap, how many values it holds
	int tupleNameLength = 3;	//length of generated key tuple names
	int NonTupleNameLength = 3;	//length of generated non-key tuple names

	int realGened = 0;
	
	public SyntheticDataGenerator4() throws SQLException 
	{
		super();
	}
	
	//===================
	//Inputs are Connection, Desired Schema, The query, Size of desired database, % of tuples to be inconsistent, max per key violations
	//
	//====================
	public void generateData(Connection connection, Schema schema, SQLQuery query, int size, int incNum, int maxVio) throws SQLException 
	{	
		ArrayList<HashMap<String, Integer>> keyTupleMaps = new ArrayList<HashMap<String, Integer>>();
		HashMap<String, Integer> strKeyTupleMap = new HashMap<String, Integer>();
		HashMap<String, Integer> douKeyTupleMap = new HashMap<String, Integer>();

		keyTupleMaps.add(strKeyTupleMap);
		keyTupleMaps.add(douKeyTupleMap);
		generateHashMap(keyTupleMaps.get(0), 0);
		generateHashMap(keyTupleMaps.get(1), 1);
		
		generateConsistent(connection, query, keyTupleMaps, schema, (size*(100-incNum))/100);
		generateInconsistent(connection, query, keyTupleMaps, schema, size - ((size*(100-incNum))/100), maxVio);

		System.out.println("Done");
	}
	
	//===================
	//Inputs are Connection to the DB, the SQLQuery that was given, the ArrayList of Hashmaps that holds the KEY lists, the Schema, and the size to be generated
	//Populates all the SQLQuery "From" relations with 'size'  consistent tuples and then inserts into the database
	//====================
	public void generateConsistent(Connection connection, SQLQuery query, ArrayList<HashMap<String, Integer>> keyTupleMaps, Schema schema, int size) 
			throws SQLException
	{
		List<PreparedStatement> dbInserts = new ArrayList<PreparedStatement>();
		Set<String> set = new HashSet<String>(query.getFrom());
		List<Relation> newRelations = new ArrayList<Relation>(schema.getRelationsByNames(set));
		//For every relation, create a new table in the DB and add a prepared statement to the List
		for (Relation atom : newRelations) 
		{
			createRelation(atom, connection);
			dbInserts.add(getInsertStatement(atom, connection));
		}
		Random intGen = new Random();
		int j = 0;
		//Iterates through every relation to be generated
		for (Relation atom : newRelations) {
			//Iterates through 'size' times to generate all the desired tuples
			for(int i = 0; i < size; i++) {
				String[] genTuple = new String[atom.getNoOfAttributes()];
				//Iterates through the length of the tuples, which == # of attributes in the relation
				for (int k = 0; k < genTuple.length; k++) {
					//Check if selected attribute is KEY and then what type it is, and populates accordingly
					int attType = Integer.parseInt(atom.getTypes().get(k));
					java.lang.Object[] holder = keyTupleMaps.get(attType).keySet().toArray();
					if(atom.getKeyAttributes().contains(k)) {
						int L = 0;
						//Will randomly select a value from the correct hashmap and populate genTuples with it
						while(genTuple[k] == null && L < 100) {
							L++;
							String randomName = (String)(holder[intGen.nextInt(realGened)]);
						    if(keyTupleMaps.get(attType).get(randomName) <= j){
						    	genTuple[k] = randomName;
						    	keyTupleMaps.get(attType).put(genTuple[k], j+1);
						    	break;
						    }
						}	
					}
					//Not a KEY col, so checks what type it is, and randomly populates it according to the globals given
					else{
						String randomName = (String)(holder[intGen.nextInt(realGened)]);
						genTuple[k] = randomName;
					}				
				}
				//Sets the prepared statement with the generated data
				for(int k = 0; k < genTuple.length; k++){
					dbInserts.get(j).setString(k+1,genTuple[k]);
				}
				//adds the prepared statement to the batch
				dbInserts.get(j).addBatch();
			}
			j++;
		}
		//updates the DB with all the generated tuples
		for (PreparedStatement dbInsert : dbInserts)
			dbInsert.executeBatch();
	}
	
	//===================
	//Inputs are Connection to the DB, the SQLQuery that was given, the ArrayList of Hashmaps that holds the KEY lists, the Schema, and the size to be generated, and maximum violations per key
	//Populates all the SQLQuery "From" relations with 'size'  inconsistent tuples and then inserts into the database
	//====================
	public void generateInconsistent(Connection connection, SQLQuery query, ArrayList<HashMap<String, Integer>> keyTupleMaps, Schema schema, int size, int maxVio)
			throws SQLException
	{
		List<PreparedStatement> dbInserts = new ArrayList<PreparedStatement>();
		Set<String> set = new HashSet<String>(query.getFrom());
		List<Relation> newRelations = new ArrayList<Relation>(schema.getRelationsByNames(set));
		for (Relation atom : newRelations) 
		{
			dbInserts.add(getInsertStatement(atom, connection));
		}
		//RandomString strGen = new RandomString(NonTupleNameLength, ThreadLocalRandom.current());
		Random intGen = new Random();
		int j = 0;
		//Iterates through every relation to be generated
		for (Relation atom : newRelations) {
			//Iterates through 'size' times to generate all the desired tuples, due to randInt tuples being generated per cylce, is iterated manually
			int i = 0;
			while(i < size) {
				//randomly chooses an int 2-maxVio
				int randInt = 2 + intGen.nextInt(maxVio-1);
				i+=randInt;
				//the first generated tuple in a violation series sets the key tuples for the rest
				boolean first = false;
				String[] keys = new String[atom.getNoOfAttributes()];
				//creates randInt # of tuples all with same key's
				for(int counter = 0; counter < randInt; counter++) {
					String[] genTuple = new String[atom.getNoOfAttributes()];
					//Iterates through the length of the tuples, which == # of attributes in the relation
					for (int k = 0; k < genTuple.length; k++) {	
						int attType = Integer.parseInt(atom.getTypes().get(k));
						java.lang.Object[] holder = keyTupleMaps.get(attType).keySet().toArray();
						//Check if selected attribute is KEY and then what type it is, and populates accordingly
						if(atom.getKeyAttributes().contains(k)) {
							if(!first) {	
								int L = 0;
								//Will randomly select a value from the correct hashmap and populate genTuples with it
								while(genTuple[k] == null && L < 100) {
									L++;
									String randomName = (String)(holder[intGen.nextInt(realGened)]);
								    if(keyTupleMaps.get(attType).get(randomName) <= j){
								    	genTuple[k] = randomName;
								    	keys[k] = randomName;
								    	keyTupleMaps.get(attType).put(genTuple[k], j+1);
								    	break;
								    }
								}
							}
							else
								genTuple[k] = keys[k];
						}
						//Not a KEY col, so checks what type it is, and randomly populates it according to the globals given
						else{
							String randomName = (String)(holder[intGen.nextInt(realGened)]);
							genTuple[k] = randomName;
						}	
					}
					//Sets the prepared statement with the generated data
					for(int k = 0; k < genTuple.length; k++){
						dbInserts.get(j).setString(k+1,genTuple[k]);
					}
					//adds the prepared statement to the batch
					dbInserts.get(j).addBatch();
					first = true;
				}
			}
			j++;
		//updates the DB with all the generated tuples
		for (PreparedStatement dbInsert : dbInserts)
			dbInsert.executeBatch();
		}
	}
	
	//===================
	//Inputs is the relation to be dropped and then created, and the connection to be used
	//Drops the relation if already exists and then creates a new one that is blank
	//====================
	private void createRelation(Relation atom, Connection connection) throws SQLException {
		connection.prepareStatement("DROP TABLE IF EXISTS " + atom.getName()).execute();
		//con.prepareStatement("DROP TABLE IF EXISTS " + "TEST").execute();
		String createQuery = "CREATE TABLE " + atom.getName() + " (";
		String prefix = "";
		for (int i = 0; i < atom.getNoOfAttributes(); i++) {
			createQuery = createQuery + prefix + atom.getAttributes().get(i) + " TEXT";
			prefix = ",";
		}
		createQuery += ")";
		connection.prepareStatement(createQuery).execute();
	}
	
	//===================
	//Inputs is the relation to be inserted into, and the connection to be used
	//Creates a PreparedStatement template for the desired relation to be used later
	//====================
	private PreparedStatement getInsertStatement(Relation atom, Connection connection) throws SQLException {
		String insertQuery = "INSERT INTO " + atom.getName() + " VALUES(";
		String prefix = "";
		for (int i = 0; i < atom.getNoOfAttributes(); i++) {
			insertQuery = insertQuery + prefix + "?";
			prefix = ",";
		}
		insertQuery += ")";
		return connection.prepareStatement(insertQuery);
	}
	//===================
	//Inputs is the HashMap to be populated, and the type of object to be placed.
	//type 0 = String, type 1 = Double
	//Populates HashMap with unique keys to be used as tuple key attributes. Calls generateNames
	//====================
	void generateHashMap(HashMap<String, Integer> tupleMap, int type)
	{
		
		if(type == 0)
			generateKeyNames(tupleMap);
		else if(type == 1)
			generateKeyValues(tupleMap);

		//for (String i : tupleMap.keySet())
		     // System.out.println("key: " + i + " value: " + tupleMap.get(i));
		System.out.println("GENERATED" + tupleMap.size());
		return;
	}

	
	//THIS BLOCK GENERATES NON-RANDOM AND CONTINOUS INTEGERS
	//INPUT IS THE INT HashMap FOR KEYS
	void generateKeyValues(HashMap<String, Integer> tupleMap)
	{		
		int sizeToGen = (int)Math.pow(52, tupleNameLength);
		if(sizeToGen > setSize || sizeToGen < 0)
			sizeToGen = setSize;
		for (int i = 0; i < sizeToGen; i++) {		
			String str = "" + i;
			tupleMap.put(str, 0);
		}
	}
	
	//THIS BLOCK GENERATES RANDOM PERMUTATIONS OF LETTERS OF LENGTH tupleNameLength.
	//INPUT IS THE STR HashMap FOR KEYS
	void generateKeyNames(HashMap<String, Integer> tupleMap)
	{
		String letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		Random gen = new Random();
		for(int j = 0; j < setSize; j++) {
			 StringBuilder strBuilder = new StringBuilder(tupleNameLength);
			 for(int k = 0 ; k < tupleNameLength; k++) {
				 int randomInt = gen.nextInt(letters.length());
				 strBuilder.append(letters.charAt(randomInt) );
			 }
			 tupleMap.put(strBuilder.toString(), 0); 
		}
		realGened = tupleMap.size();
	}
}
