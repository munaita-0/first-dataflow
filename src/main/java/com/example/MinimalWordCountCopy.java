package com.example;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Created by suzuki_shogo on 2018/07/28.
 */
public class MinimalWordCountCopy {
    public static void main(String[] args) {
//        PipelineOptions options = PipelineOptionsFactory.create();
//        Pipeline p = Pipeline.create(options);
//        System.out.println(p.toString());
//        p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"));
//        System.out.println(p.toString());
//        System.out.println("==========");
//        p.apply(TextIO.write().to("wordcounts"));
    }
}
