package com.qat.samples.kafka;

import lombok.Data;
import org.springframework.data.annotation.Id;

/**
 * Created by rpulley on 2/10/17.
 */
@Data
public class DocCodeReportItem {
    @Id
    private String docCode;

    private Long count;
}
