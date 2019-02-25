package com.util;

import java.util.HashSet;
import java.util.Set;

public class Test2 {

	public static void main(String[] args) {
		Set<String> set = new HashSet<String>();
		set.add("Akhil");
		set.add("Akhil");
		set.add("AKhil");
		System.out.println(set);
	}
}
