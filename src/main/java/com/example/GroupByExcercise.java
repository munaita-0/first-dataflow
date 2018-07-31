package com.example;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

/**
 * Created by suzuki_shogo on 2018/07/29.
 * reference: https://qiita.com/Sekky0905/items/941ad819f00390a6929e
 */
public class GroupByExcercise {

    static class SplitWordsAndMakeKVFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] words = c.element().split(",");
            c.output(KV.of(words[0], Integer.parseInt(words[1])));
        }
    }

    static class TransTypeFromKVAndMakeStringFn extends DoFn<KV<String, Iterable<Integer>>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(String.valueOf(c.element()));
        }
    }

    private static final String INPUT_FILE_PATH = "./input_files/group_by_sample.txt";
    private static final String OUTPUT_FILE_PATH = "./output_files/group_by_sample.txt";

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
        pipeline.apply(TextIO.read().from(INPUT_FILE_PATH))
                .apply(ParDo.of(new SplitWordsAndMakeKVFn()))
                .apply(GroupByKey.<String, Integer>create())
                .apply(ParDo.of(new TransTypeFromKVAndMakeStringFn()))
                .apply(TextIO.write().to(OUTPUT_FILE_PATH));
        pipeline.run().waitUntilFinish();
    }
}
