package edu.rit.ibd.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamGobbler extends Thread {
	private InputStream is;
	private boolean print;

	public StreamGobbler(InputStream is, boolean print) {
		this.is = is;
		this.print = print;
	}

	public void run() {
		try {
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			while ((line = br.readLine()) != null)
				if (print)
					System.out.println(line);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
