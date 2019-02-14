package com.util;

public class Test1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
String s = "{c}";
System.out.println(s.startsWith("{") && s.endsWith("}"));
System.out.println(s.replaceAll("\\{", "d"));
	}

}
