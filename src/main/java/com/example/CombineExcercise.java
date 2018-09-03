package com.example;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;


/**
 * Created by suzuki_shogo on 2018/07/29.
 * reference: https://qiita.com/Sekky0905/items/4596660455a7a2af5906
 */
public class CombineExcercise {

    static class TransformTypeFromStringToInteger extends DoFn<String, Integer> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(Integer.parseInt(c.element()));
        }
    }

    static class TransformTypeFromIntegerToString extends DoFn<Integer, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(String.valueOf(c.element()));
        }
    }

    private static final String INPUT_FILE_PATH = "./input_files/combine_by_sample.txt";
    private static final String OUTPUT_FILE_PATH = "./output_files/combine_by_sample.txt";

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
        PCollection<String> lines = pipeline.apply(TextIO.read().from(INPUT_FILE_PATH));
        PCollection<Integer> integerPCollection = lines.apply(ParDo.of(new TransformTypeFromStringToInteger()));
        PCollection<Integer> sum = integerPCollection.apply(Sum.integersGlobally().withoutDefaults());
        PCollection<String> numString = sum.apply(ParDo.of(new TransformTypeFromIntegerToString()));
        numString.apply(TextIO.write().to(OUTPUT_FILE_PATH));
        pipeline.run().waitUntilFinish();
    }
}
