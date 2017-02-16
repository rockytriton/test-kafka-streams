package com.qat.samples.kafka.bac.impl;

import com.qat.samples.kafka.bac.IReportBAC;
import com.qat.samples.kafka.model.DocCodeReportItem;
import com.qat.samples.kafka.processors.PageCountProcessor;
import com.qat.samples.kafka.repo.DocCodeReportRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by rpulley on 2/16/17.
 */
@Component
public class ReportBACImpl implements IReportBAC {
    @Autowired
    DocCodeReportRepository repo;

    @Autowired
    PageCountProcessor pcProcessor;

    public List<DocCodeReportItem> fetchAll() {
        return repo.findAll();
    }

    public Long fetchPageCount() {
        return pcProcessor.getPageCounts();
    }
}
