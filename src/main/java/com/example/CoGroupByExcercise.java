package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by suzuki_shogo on 2018/07/29.
 * reference: https://github.com/jbonofre/beam-samples/blob/fa45e9d4bee9acedc657eec71a41585582cd238e/join/src/main/java/org/apache/beam/samples/join/JoinInBatch.java
 */
public class CoGroupByExcercise {
    private static final String OUTPUT_FILE_PATH = "./output_files/co_group_by_sample.txt";

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        List<KV<String, String>> data1 = new ArrayList<>();
        data1.add(KV.of("key_1", "value_1"));
        data1.add(KV.of("key_2", "value_2"));
        PCollection<KV<String, String>> input1 = pipeline.apply(Create.of(data1));

        List<KV<String, Integer>> data2 = new ArrayList<>();
        data2.add(KV.of("key_1", 1));
        data2.add(KV.of("key_2", 2));
        PCollection<KV<String, Integer>> input2 = pipeline.apply(Create.of(data2));

        final TupleTag<String> tag1 = new TupleTag<>();
        final TupleTag<Integer> tag2 = new TupleTag<>();

        KeyedPCollectionTuple.of(tag1, input1)
                .and(tag2, input2)
                .apply(CoGroupByKey.<String>create())
                .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>(){
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                        StringBuilder builder = new StringBuilder();
                        KV<String, CoGbkResult> element = processContext.element();
                        builder.append(element.getKey()).append(": [");
                        Iterable<String> tag1Val = element.getValue().getAll(tag1);
                        for (String val : tag1Val) {
                            builder.append(val).append(",");
                        }
                        Iterable<Integer> tag2Val = element.getValue().getAll(tag2);
                        for (Integer val: tag2Val) {
                            builder.append(val).append("");
                        }
                        builder.append("]");
                        processContext.output(builder.toString());
                    }
                }))
                .apply(TextIO.write().to(OUTPUT_FILE_PATH));

        pipeline.run().waitUntilFinish();
    }
}
