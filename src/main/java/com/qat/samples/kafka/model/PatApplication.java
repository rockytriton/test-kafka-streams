package com.qat.samples.kafka.model;

import lombok.Data;
import org.springframework.data.annotation.Id;

/**
 * Created by rpulley on 2/10/17.
 */
@Data
public class PatApplication {
    @Id
    private String applicationId;


}
