package com.qat.samples.kafka;

import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Created by rpulley on 2/10/17.
 */
public interface DocCodeReportRepository extends MongoRepository<DocCodeReportItem, String> {
}
