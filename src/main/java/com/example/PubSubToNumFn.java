package com.example;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.KV;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubToNumFn extends DoFn<String, KV<String, Double>> {

    private final Logger LOG = LoggerFactory.getLogger(PubSubToNumFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        String json = c.element();
        Gson gson = new Gson();
        LOG.info(json);
        SensorData data = gson.fromJson(json, SensorData.class);
        if (data == null) {
            LOG.error("data is null");
        } else {
            LOG.info(data.getTemperature());
        }
        c.output(KV.of("temperature", Double.valueOf(data.getTemperature())));
    }
}

