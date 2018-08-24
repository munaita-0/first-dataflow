// ref: https://github.com/spotify/scio/blob/master/scio-examples/src/main/java/org/apache/beam/examples/cookbook/TriggerExample.java

package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.example.common.ExampleUtils;
import com.example.common.ExampleBigQueryTableOptions;
import com.example.common.ExampleOptions;

import java.awt.image.AreaAveragingScaleFilter;
import java.io.IOException;
import java.text.Format;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericData.Fixed;
import org.apache.beam.runners.direct.repackaged.model.pipeline.v1.RunnerApi.Trigger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;



import org.joda.time.Duration;
import org.joda.time.Instant;

public class TriggerExample {
  public static final int WINDOW_DURATION = 30;

  static class CalculateTotalFlow
  extends PTransform <PCollection<KV<String, Integer>>, PCollectionList<TableRow>> {
    private int windowDuration;

    CalculateTotalFlow(int windowDuration) {
      this.windowDuration = windowDuration;
    }

    @Override
    public PCollectionList<TableRow> expand(PCollection<KV<String, Integer>> flowInfo) {
      PCollection<TableRow> defaultTriggerResults = flowInfo
        .apply("Default", Window
        .<KV<String, Integer>>into(FixedWindows.of(Duration.standardMinutes(windowDuration)))
        .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
        .withAllowedLateness(Duration.ZERO)
        .discardingFiredPanes())
        .apply(new TotalFlow("default"));
      
      PCollectionList<TableRow> resultList = PCollectionList.of(defaultTriggerResults);
      //   .and(withAllowedLatenessResults)
      //   .and(speculartiveResults)
      //   .and(sequentialResults);

      return resultList;
    }
  }

  static class TotalFlow extends PTransform<PCollection<KV<String, Integer>>, PCollection<TableRow>> {
    private String triggerType;

    public TotalFlow(String triggerType) {
      this.triggerType = triggerType;
    }

    @Override
    public PCollection<TableRow> expand(PCollection<KV<String, Integer>> flowInfo) {
      PCollection<KV<String, Iterable<Integer>>> flowPerFreeway = flowInfo.apply(GroupByKey.create());

      PCollection<KV<String, String>> results = flowPerFreeway.apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<String, String>>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          Iterable<Integer> flows = c.element().getValue();
          Integer sum = 0;
          Long numberOfRecords = 0l;
          for (Integer value: flows) {
            sum += value;
            numberOfRecords++;
          }
          c.output(KV.of(c.element().getKey(), sum + "," + numberOfRecords));
        }
      }));

      PCollection<TableRow> output = results.apply(ParDo.of(new FormatTotalFlow(triggerType)));
      return output;
    }
  }

  static class FormatTotalFlow extends DoFn<KV<String, String>, TableRow> {
      private String triggerType;

      public FormatTotalFlow(String triggerType) {
        this.triggerType = triggerType;
      }

      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
        String[] values = c.element().getValue().split(",", -1);
        TableRow row = new TableRow()
            .set("trigger_type", triggerType)
            .set("freeway", c.element().getKey())
            .set("total_flow", Integer.parseInt(values[0]))
            .set("number_of_records", Long.parseLong(values[1]))
            .set("window", window.toString())
            .set("isFirst", c.pane().isFirst())
            .set("isLast", c.pane().isLast())
            .set("timing", c.pane().getTiming().toString())
            .set("event_time", c.timestamp().toString())
            .set("processing_time", Instant.now().toString());
        c.output(row);
      }
  }

  static class ExtractFlowInfo extends DoFn<String, KV<String, Integer>> {
    private static final int VALID_NUM_FIELDS = 50;

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String[] laneInfo = c.element().split(",", -1);
      if ("timestamp".equals(laneInfo[0])) {
        return;
      }
      if (laneInfo.length < VALID_NUM_FIELDS) {
        return;
      }

      String freeway = laneInfo[2];
      Integer totalFlow = tryIntegerParse(laneInfo[7]);
      if (totalFlow == null || totalFlow <= 0) {
        return;
      }
      c.output(KV.of(freeway, totalFlow));
    }
  }

  // 1%未満で遅延させる
  public static class InsertDelays extends DoFn<String, String> {
    private static final double THRESHOLD = 0.001;
    private static final int MIN_DELAY = 1;
    private static final int MAX_DELAY = 100;

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Instant timestamp = Instant.now();
      Random random = new Random();
      if (random.nextDouble() < THRESHOLD) {
        int range = MAX_DELAY - MIN_DELAY;
        int delayInMinutes = random.nextInt(range) + MIN_DELAY;
        long delayInMillis = TimeUnit.MINUTES.toMillis(delayInMinutes);
        timestamp = new Instant(timestamp.getMillis() - delayInMillis);
      }
      c.outputWithTimestamp(c.element(), timestamp);
    }
  }

  public interface TrafficFlowOptions extends ExampleOptions, ExampleBigQueryTableOptions, StreamingOptions {
    @Description("Input file to read from")
    @Default.String("gs://apache-beam-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15.csv")
    String getInput();

    void setInput(String value);

    @Description("Numeric value of window duration for fixed windows, in minutes")
    @Default.Integer(WINDOW_DURATION)
    Integer getWindowDuration();

    void setWindowDuration(Integer value);
  }

  public static void main(String[] args) throws Exception {
    TrafficFlowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(TrafficFlowOptions.class);
    options.setStreaming(true);
    options.setBigQuerySchema(getSchema());
    ExampleUtils exampleUtils = new ExampleUtils(options);
    exampleUtils.setup();

    Pipeline pipeline = Pipeline.create(options);

    TableReference tableRef = getTableReference(options.getProject(), options.getBigQueryDataset(),
        options.getBigQueryTable());

    PCollectionList<TableRow> resultList = pipeline.apply("ReadMyFile", TextIO.read().from(options.getInput()))
        .apply("InsertRandomDelays", ParDo.of(new InsertDelays()))
        .apply(ParDo.of(new ExtractFlowInfo()))
        .apply(new CalculateTotalFlow(options.getWindowDuration()));

    for (int i = 0; i < resultList.size(); i++) {
      resultList.get(i).apply(BigQueryIO.writeTableRows().to(tableRef).withSchema(getSchema()));
    }

    PipelineResult result = pipeline.run();

    exampleUtils.waitToFinish(result);
  }

  private static TableSchema getSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("trigger_type").setType("STRING"));
    fields.add(new TableFieldSchema().setName("freeway").setType("STRING"));
    fields.add(new TableFieldSchema().setName("total_flow").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("number_of_records").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("window").setType("STRING"));
    fields.add(new TableFieldSchema().setName("isFirst").setType("BOOLEAN"));
    fields.add(new TableFieldSchema().setName("isLast").setType("BOOLEAN"));
    fields.add(new TableFieldSchema().setName("timing").setType("STRING"));
    fields.add(new TableFieldSchema().setName("event_time").setType("TIMESTAMP"));
    fields.add(new TableFieldSchema().setName("processing_time").setType("TIMESTAMP"));
    TableSchema schema = new TableSchema().setFields(fields);
    return schema;
  }

  private static TableReference getTableReference(String project, String dataset, String table) {
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(project);
    tableRef.setDatasetId(dataset);
    tableRef.setTableId(table);
    return tableRef;
  }

  private static Integer tryIntegerParse(String number) {
    try {
      return Integer.parseInt(number);
    } catch(NumberFormatException e) {
      return null;
    }
  }
}