package dev.irontools.flink.serde;

import dev.irontools.flink.serde.json.FastJsonDeserializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Thread)
public class FastJsonDeserializerPojoBenchmark {
  
  public static class Customer {
    public int id;
    public String first_name;
    public String last_name;
    public String address;
    public String country;
    public String status;
    public long balance;
  }
  
  @State(Scope.Benchmark)
  public static class JsonDeserializationState {
    public byte[] data;
    
    public DeserializationSchema<Customer> deserializationSchema;
    public DeserializationSchema<Customer> fastDeserializationSchema;
    
    @Setup(Level.Trial)
    public void setUp() {
      deserializationSchema = new JsonDeserializationSchema<>(Customer.class);
      fastDeserializationSchema = new FastJsonDeserializationSchema<>(Customer.class);
      
      try {
        deserializationSchema.open(null);
        fastDeserializationSchema.open(null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      
      data =
        "{\"id\":1234538243,\"first_name\":\"John\",\"last_name\":\"Doe\",\"address\":\"123 Main St\",\"country\":\"USA\",\"status\":\"active\",\"balance\":100000000}"
          .getBytes(StandardCharsets.UTF_8);
    }
  }
  
  @Benchmark
  public Customer measureJsonDeserializationStandard(JsonDeserializationState scenario)
    throws IOException {
    return scenario.deserializationSchema.deserialize(scenario.data);
  }
  
  @Benchmark
  public Customer measureFastJsonDeserializationStandard(JsonDeserializationState scenario)
    throws IOException {
    return scenario.fastDeserializationSchema.deserialize(scenario.data);
  }
  
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .include(FastJsonDeserializerPojoBenchmark.class.getSimpleName())
      .addProfiler(JavaFlightRecorderProfiler.class)
      .build();
    
    new Runner(opt).run();
  }
}
