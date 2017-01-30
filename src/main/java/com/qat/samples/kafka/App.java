package com.qat.samples.kafka;

public class App {
	public static void main(String[] args) {
		new IncomingDocProcessor().start();
		new DocCodeReportConsumer().start();
		new ReportProcessor().start();
		//new ReportProcessorPages().start();
	}
}
