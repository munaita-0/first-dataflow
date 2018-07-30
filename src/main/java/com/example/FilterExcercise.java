package com.example;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;


/**
 * Created by suzuki_shogo on 2018/07/29.
 * reference: https://qiita.com/Sekky0905/items/381ed27fca9a16f8ef07
 */
public class FilterExcercise {
    static class FilterEvenFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element());
            int num = Integer.parseInt(c.element());
            if (num % 2 == 0) {
                System.out.println("ifの結果" + num);
                c.output(String.valueOf(num));
            }
        }
    }


    private static final String INPUT_FILE_PATH = "./dataflow_number_test.csv";
    private static final String OUTPUT_FILE_PATH = "./sample.csv";

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
        pipeline.apply(TextIO.read().from(INPUT_FILE_PATH))
                .apply(ParDo.of(new FilterEvenFn()))
                .apply(TextIO.write().to(OUTPUT_FILE_PATH));
        pipeline.run().waitUntilFinish();
    }
}
