package com.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Test {

	public static void main(String[] args) throws IOException {
		Charset charset = Charset.forName("ISO-8859-1");
		List<String> lines = Files.readAllLines(Paths.get("C:/Users/Akhil/formula1.txt"), charset);
		lines.set(0, "Akhil");
		StringBuilder builder = new StringBuilder();
		for (String line : lines) {
			if (line.equals("Akhil"))
				System.out.println("Line is Akhil");
			builder.append(line + System.lineSeparator());
		}
		Files.write(Paths.get("C:/Users/Akhil/formula2.txt"), builder.toString().getBytes());
	}
}
