// ref: https://www.apps-gcp.com/cloud-dataflow/#Java

package com.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import com.example.common.WriteOneFilePerWindow;
import org.joda.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.TextIO;
import com.example.common.WindowedFilenamePolicy;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;


public class PubsubIoExample {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubIoExample.class);
  public static final Duration ONE_DAY = Duration.standardDays(1);

  static class AddTimestampFn extends DoFn<String, String> {
    private final Instant minTimestamp;
    private final Instant maxTimestamp;

    AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
      this.minTimestamp = minTimestamp;
      this.maxTimestamp = maxTimestamp;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Instant randomTimestamp = new Instant(
          ThreadLocalRandom.current().nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis())); c.outputWithTimestamp(c.element(), new Instant(randomTimestamp)); } } 

  public interface PubsubIoOptions extends DataflowPipelineOptions {
    String getFromTopic();
    void setFromTopic(String fromTopic);

    String getOutput();
    void setOutput(String output);

    @Description("The directory to output files to. Must end with a slash.")
    @Required
    ValueProvider<String> getOutputDirectory();
    void setOutputDirectory(ValueProvider<String> value);
  }

  public static void main(String[] args) {
    PubsubIoOptions options = PipelineOptionsFactory.fromArgs(args)
      .withValidation().as(PubsubIoOptions.class);

    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> p = pipeline.apply("read from Pubsub", PubsubIO.readStrings().fromTopic(options.getFromTopic()))
        .apply(ParDo.of(new AddTimestampFn(
                   new Instant(System.currentTimeMillis() + 30000),
                   new Instant(System.currentTimeMillis() + Duration.standardHours(1).getMillis())
                        )
              ));
    PCollection<String> p2 = p.apply( "window",
      Window.<String>into(
        FixedWindows.of(Duration.standardMinutes(1))
      ).triggering(
        Repeatedly.forever(AfterWatermark.pastEndOfWindow())
      ).discardingFiredPanes().withAllowedLateness(ONE_DAY)
    );

    String outputDir = "gs://suzuki-test-sb/output/";

    p2.apply(
      "Write Files",
      TextIO.write()
        .withWindowedWrites()
        .withNumShards(1)
        .to(
          new WindowedFilenamePolicy(
            outputDir,
            "pubsub_io",
            "SSS-NNN",
            ""))
        .withTempDirectory(NestedValueProvider.of(
          options.getOutputDirectory(), 
          (SerializableFunction<String, ResourceId>) input -> 
            FileBasedSink.convertToFileResourceIfPossible(input))));
    
    // apply(new WriteOneFilePerWindow(options.getOutput(), 1));
    pipeline.run().waitUntilFinish();
  }
}