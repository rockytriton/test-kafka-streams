package com.qat.samples.kafka.bac;

import com.qat.samples.kafka.model.DocCodeReportItem;

import java.util.List;

/**
 * Created by rpulley on 2/16/17.
 */
public interface IReportBAC {
    List<DocCodeReportItem> fetchAll();
    Long fetchPageCount();
}
