package com.qat.samples.kafka;
import java.time.LocalDate;
import java.util.Date;

import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
public class PatDocument {
	@Id
	private String documentId;

	private String applicationId;

	private String docCode;
	private Integer numPages;
	private String sourceSystem;
	private String sourceId;
	private String submissionId;
	private Date officialDate;
	
}
