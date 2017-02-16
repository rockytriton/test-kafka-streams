package com.qat.samples.kafka.services;

import com.qat.samples.kafka.bac.IReportBAC;
import com.qat.samples.kafka.model.DocCodeReportItem;
import com.qat.samples.kafka.model.PageCountResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.List;

/**
 * Created by rpulley on 2/16/17.
 */
@Controller
public class DocCodeReportController {
    @Autowired
    IReportBAC bacLayer;

    @RequestMapping(value="/services/v1/reports/docCodes", method= RequestMethod.GET)
    public ResponseEntity<List<DocCodeReportItem>> fetchDocCodeReport() {
        return new ResponseEntity<List<DocCodeReportItem>>(bacLayer.fetchAll(), HttpStatus.OK);
    }

    @RequestMapping(value="/services/v1/reports/pageCount", method= RequestMethod.GET)
    public ResponseEntity<PageCountResponse> fetchPageCount() {
        PageCountResponse resp = new PageCountResponse();
        resp.setPageCount(bacLayer.fetchPageCount());
        return new ResponseEntity<>(resp, HttpStatus.OK);
    }


}
