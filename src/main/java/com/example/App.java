package com.example;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.gson.Gson;

import org.joda.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App 
{
    public static void main( String[] args )
    {
        DataflowPipelineOptions options = PipelineOptionsFactory
		.fromArgs(args)
		.withValidation()
		.create()
                .as(DataflowPipelineOptions.class);
        options.setStreaming(true);
        options.setJobName("test");

        Pipeline p = Pipeline.create(options);
        PCollection<KV<String, Double>> scores = p.apply(PubsubIO.readStrings().fromTopic("projects/project-id/topics/raspi3"))
            .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(2))))
            .apply(ParDo.of(new PubSubToNumFn()))
            .apply(Sum.<String>doublesPerKey())
            .apply(ParDo.of(new LoggingFn()));

        p.run();
    }

    public static class LoggingFn extends DoFn<KV<String, Double>, KV<String, Double>> {

        private static final Logger LOG = LoggerFactory.getLogger(LoggingFn.class);

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, Double> kv = c.element();
            double sum = kv.getValue();

            LOG.info(String.valueOf(sum));
            c.output(kv);
        }
    }
}
