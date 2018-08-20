package com.example;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DebuggingWordCount {
    public static class FilterTextFn extends DoFn<KV<String, Long>, KV<String, Long>> {

        private static final Logger LOG = LoggerFactory.getLogger(FilterTextFn.class);
        private final Pattern filter;

        public FilterTextFn(String pattern) {
            filter = Pattern.compile(pattern);
        }

        private final Counter matchedWords = Metrics.counter(FilterTextFn.class, "matchedWords");
        private final Counter unmatchedWords = Metrics.counter(FilterTextFn.class, "unmatchedWords");

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (filter.matcher(c.element().getKey()).matches()) {
                LOG.error("Matched: " + c.element().getKey());
                matchedWords.inc();
                c.output(c.element());
            } else {
                LOG.debug("Did not match: " + c.element().getKey());
                unmatchedWords.inc();
            }
        }
    }

    public interface WordCountOptions extends WordCount.WordCountOptions {
        @Description("Regex filter pattern to use in DebuggingWordCount. "
          + "Only words matching this pattern will be counted.")
        @Default.String("Flourish|stomach")
        String getFilterPattern();
        void setFilterPattern(String value);
    }

    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
          .as(WordCountOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollection<KV<String, Long>> filteredWords = 
          p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
           .apply(new WordCount.CountWords())
           .apply(ParDo.of(new FilterTextFn(options.getFilterPattern())));
    
        List<KV<String, Long>> expectedResults = Arrays.asList(
            KV.of("Flourish", 3L),
            KV.of("stomach", 1L));
        PAssert.that(filteredWords).containsInAnyOrder(expectedResults);

        p.run().waitUntilFinish();
    }
}