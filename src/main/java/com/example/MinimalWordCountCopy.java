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
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by suzuki_shogo on 2018/07/28.
 */
public class MinimalWordCountCopy {
    public static void main(String[] args) {
       PipelineOptions options = PipelineOptionsFactory.create();
       Pipeline p = Pipeline.create(options);
       System.out.println(p.toString());

       PCollection<String> p2 = p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"));
       PCollection<String> p3 =  p2.apply(FlatMapElements
            .into(TypeDescriptors.strings())
            .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))));

       PCollection<String> p4 =  p3.apply(Filter.by((String word) -> !word.isEmpty()));

       PCollection<KV<String, Long>> p5 = p4.apply(Count.perElement());

       PCollection<String> p6 = p5.apply(MapElements
            .into(TypeDescriptors.strings())
            .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()));

       p6.apply(TextIO.write().to("output_files/minimal"));

       p.run().waitUntilFinish();
    }
}
