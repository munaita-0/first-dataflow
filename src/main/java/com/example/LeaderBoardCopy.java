package com.example;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import com.example.common.ExampleOptions;
import com.example.common.ExampleUtils;
import com.example.complete.game.utils.GameConstants;
import com.example.complete.game.utils.WriteToBigQuery;
import com.example.complete.game.utils.WriteWindowedToBigQuery;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class LeaderBoardCopy extends HourlyTeamScoreCopy {

  static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  static final Duration TEN_MINUTES = Duration.standardMinutes(10);

  interface Options extends HourlyTeamScoreCopy.Options, ExampleOptions, StreamingOptions {
    @Description("BigQuery Dataset to write tables to. Must already exist.")
    @Validation.Required
    String getDataset();
    void setDataset(String value);

    @Description("Pub/Sub topic to read from")
    @Validation.Required
    String getTopic();
    void setTopic(String value);

    @Description("Numeric value of fixed window duration for team analysis, in minutes")
    @Default.Integer(60)
    Integer getTeamWindowDuration();
    void setTeamWindowDuration(Integer value);

    @Description("Numeric value of allowed data lateness, in minutes")
    @Default.Integer(120)
    Integer getAllowedLateness();
    void setAllowedLateness(Integer value);

    @Description("Prefix used for the BigQuery table names")
    @Default.String("leaderboard")
    String getLeaderBoardTableName();
    void setLeaderBoardTableName(String value);
  }

  @VisibleForTesting
  static class CalculateTeamScores extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {
    private final Duration teamWindowDuration;
    private final Duration allowedLateness;

    CalculateTeamScores(Duration teamWindowDuration, Duration allowedLateness) {
      this.teamWindowDuration = teamWindowDuration;
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> infos) {
      return infos.apply("LeaderboardTeamFixedWindows",
          Window.<GameActionInfo>into(FixedWindows.of(teamWindowDuration))
              .triggering(AfterWatermark.pastEndOfWindow()
                  .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(FIVE_MINUTES))
                  .withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(TEN_MINUTES)))
              .withAllowedLateness(allowedLateness)
              .accumulatingFiredPanes())
          .apply("ExtractTeamScore", new ExtractAndSumScore("team"));
    }
  }

  protected static Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>> configureBigQueryWrite() {
    Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure = new HashMap<>();
    tableConfigure.put(
        "user", new WriteToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getKey()));
    tableConfigure.put(
        "total_score",
        new WriteToBigQuery.FieldInfo<>("INTEGER", (c, w) -> c.element().getValue()));
    return tableConfigure;
  }

  protected static Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>> configureGlobalWindowBigQueryWrite() {
    Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure = configureBigQueryWrite();
    tableConfigure.put(
        "processing_time",
        new WriteToBigQuery.FieldInfo<>(
            "STRING",
            (c, w) -> GameConstants.DATE_TIME_FORMATTER.print(Instant.now())));
    return tableConfigure;
  }

  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);
    ExampleUtils exampleUtils = new ExampleUtils(options);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<GameActionInfo> gameEvents = pipeline
        .apply(PubsubIO.readStrings()
            .withTimestampAttribute(GameConstants.TIMESTAMP_ATTRIBUTE)
            .fromTopic(options.getTopic()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()));
    
    gameEvents
        .apply(
            "CulculateTeamScores",
            new CalculateTeamScores(
                Duration.standardMinutes(options.getTeamWindowDuration()),
                Duration.standardMinutes(options.getAllowedLateness())))
        .apply(
            "WriteTeamScoreSums",
            new WriteToBigQuery<>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getLeaderBoardTableName() + "_user",
                configureGlobalWindowBigQueryWrite()));
    
    PipelineResult result = pipeline.run();
    exampleUtils.waitToFinish(result);
  }
}