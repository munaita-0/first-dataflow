package com.example;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import com.example.common.ExampleBigQueryTableOptions;
import com.example.common.ExampleOptions;
import com.example.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;



public class WindowedWordCount {
    static final int WINDOW_SIZE = 10;

    static class AddTimestampFn extends DoFn<String, String> {
        private final Instant minTimestamp;
        private final Instant maxTimestamp;

        AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Instant randomTimestamp =
              new Instant(
                  ThreadLocalRandom.current()
                    .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis())
              );
            c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
        }

    }

    public static class DefaultToCurrentSystemTime implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return System.currentTimeMillis();
        }
    }

    public static class DefaultToMinTimestampPlusOneHour implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return options.as(Options.class).getMinTimestampMillis()
            + Duration.standardHours(1).getMillis();
        }
    }

    public interface Options extends WordCount.WordCountOptions, ExampleOptions, ExampleBigQueryTableOptions {
        @Description("Fixed window duration, in minutes")
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToCurrentSystemTime.class)
        Long getMinTimestampMillis();
        void setMinTimestampMillis(Long value);

        @Description("Maximum randomly assigned timestamp in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToMinTimestampPlusOneHour.class)
        Long getMaxTimestampMillis();
        void setmaxTimestampMillis(Long value);

        @Description("Fixed number of shards produced per window")
        Integer getNumShards();
        void setNumShards(Integer numShards);
    }

    public static void main(String[] args) throws IOException {
      Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
      final String output = options.getOutput();
      final Instant minTimestamp = new Instant(options.getMinTimestampMillis());
      final Instant maxTimestamp = new Instant(options.getMaxTimestampMillis());

      Pipeline pipeline = Pipeline.create(options);

      PCollection<String> input = pipeline
        .apply(TextIO.read().from(options.getInputFile()))
        .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));

      // 10分毎にwindowできるやつ
      PCollection<String> windowedWords = input.apply(
          Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))) 
      );

      PCollection<KV<String, Long>> wordCounts = windowedWords.apply(new WordCount.CountWords());

      wordCounts
        .apply(MapElements.via(new WordCount.FormatAsTextFn()))
        .apply(new WriteOneFilePerWindow(output, options.getNumShards()));
    
      PipelineResult result = pipeline.run();
      try {
          result.waitUntilFinish();
      } catch (Exception exc) {
          result.cancel();
      }
    }
} 