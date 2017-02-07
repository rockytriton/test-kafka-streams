package com.qat.samples.kafka;

public class App {
	public static void main(String[] args) throws Exception {
		new IncomingDocProcessor().start();
		new DocCodeReportConsumer().start();
		Thread.sleep(10000);
		//new ReportProcessor().start();
		new ReportProcessorPages().start();
	}
}
