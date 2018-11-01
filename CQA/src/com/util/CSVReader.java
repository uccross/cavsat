package com.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CSVReader {

	public static void main(String[] args) {
		File folder = new File("C:\\Users\\Akhil\\Downloads\\Physician_Compare");
		File[] arr = folder.listFiles();
		for (File file : arr) {
			printList(readColumnNames(file));
		}
	}

	public static List<String> readColumnNames(File file) {
		System.out.println(file.getName());
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();
			reader.close();
			if (null != line) {
				return Arrays.stream(line.split(",")).collect(Collectors.toList());
			}
		} catch (Exception e) {
			System.out.println("Problem reading file.");
		}
		return null;
	}

	private static void printList(List<String> list) {
		for (String s : list) {
			System.out.println(s);
		}
		System.out.println("------------------------------------------");
	}

	public List<String> readFirstRecord(String filename) {
		try {
			BufferedReader file = new BufferedReader(new FileReader(filename));
			for (int i = 0; i < 1000; i++)
				file.readLine();
			String line = file.readLine();
			file.close();
			if (null != line) {
				return Arrays.stream(line.split(",")).collect(Collectors.toList());
			}
		} catch (Exception e) {
			System.out.println("Problem reading file.");
		}
		return null;
	}

	public void fillResourcesTable(String filename) {
		try {
			BufferedReader file = new BufferedReader(new FileReader(filename));
			String insertQuery = "INSERT INTO RESOURCES (ProjectID, ResourceItemName, ResourceQuantity, ResourceUnitPrice, ResourceVendorName) VALUES (?,?,?,?,?)";
			Connection con = new DBEnvironment().getConnection();
			con.setAutoCommit(false);
			PreparedStatement psInsert = con.prepareStatement(insertQuery);
			String line;
			String[] arr;
			int count = 0, batchsize = 10000;
			while ((line = file.readLine()) != null) {
				arr = line.split(",");
				if (arr.length == 5) {
					for (int i = 0; i < 5; i++) {
						psInsert.setString(i + 1, arr[i]);
					}
					psInsert.addBatch();
					count++;
				}
				if (count % batchsize == 0) {
					psInsert.executeBatch();
					con.commit();
					System.out.println(count + " rows completed");
				}
			}
			file.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Problem reading file.");
		}
	}

}
