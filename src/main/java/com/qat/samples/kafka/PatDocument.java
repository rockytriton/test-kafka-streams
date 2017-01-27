package com.qat.samples.kafka;
import java.util.Date;

import lombok.Data;

@Data
public class PatDocument {
	private String documentId;
	private String docCode;
	private Integer numPages;
	private String sourceSystem;
	private String sourceId;
	private String submissionId;
	private Date officialDate;
	
}
