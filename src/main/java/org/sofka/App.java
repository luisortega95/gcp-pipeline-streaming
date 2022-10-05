package org.sofka;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;



public class App {

    public static void main(String[] args) {
        System.out.println(args);
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        options.setProject("low-code-dataflow");
        options.setRegion("us-central1");

        options.setStagingLocation("gs://low-code-dataflow-bucket/binaries/");
        options.setGcpTempLocation("gs://low-code-dataflow-bucket/temp/");
        options.setNetwork("default");
        options.setSubnetwork("regions/us-central1/subnetworks/default");
        options.setRunner(DataflowRunner.class);
        options.setNumWorkers(3);

        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read()
                .from("gs://low-code-dataflow-bucket/gcp-file-in/*.txt")
                .watchForNewFiles(
                        Duration.standardMinutes(1),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardHours(1))))
                .apply(new SplitWords())
                .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(new CountWords())
                .apply("FormatResults", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
                .apply("WriteCounts", TextIO.write().to("gs://low-code-dataflow-bucket/gcp-file-out/")
                        .withWindowedWrites().withNumShards(1));

        p.run().waitUntilFinish();

    }
}
