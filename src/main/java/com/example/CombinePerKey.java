package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * reference: https://qiita.com/Sekky0905/items/4596660455a7a2af5906
 */
public class CombinePerKey {
  private static final String INPUT_FILE_PATH = "./input_files/kv_combine_by_sample.txt";
  private static final String OUTPUT_FILE_PATH = "./output_files/kv_combine_by_sample.txt";

  static class SplitWordsAndMakeKVFn extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void ProcessElement(ProcessContext c) {
      String[] words = c.element().split(",");
      c.output(KV.of(words[0], Integer.parseInt(words[1])));
    }
  }

  static class TransTypeFromKVAndMakeStringFn extends DoFn<KV<String, Integer>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        // inputをString型に変換する
        c.output(String.valueOf(c.element()));
    }
}

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);
    PCollection<String> lines = pipeline.apply(TextIO.read().from(INPUT_FILE_PATH));
    PCollection<KV<String, Integer>> kvCounter = lines.apply(ParDo.of(new SplitWordsAndMakeKVFn()));
    PCollection<KV<String, Integer>> sumPerKey = kvCounter.apply(Sum.integersPerKey());
    PCollection<String> output = sumPerKey.apply(ParDo.of(new TransTypeFromKVAndMakeStringFn()));
    output.apply(TextIO.write().to(OUTPUT_FILE_PATH));
    pipeline.run().waitUntilFinish();
  }
}