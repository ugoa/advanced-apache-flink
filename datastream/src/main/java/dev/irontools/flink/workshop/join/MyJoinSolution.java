package dev.irontools.flink.workshop.join;

import dev.irontools.flink.Order;
import dev.irontools.flink.OrderGenerator;
import dev.irontools.flink.Product;
import dev.irontools.flink.ProductGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;


public class MyJoinSolution {
  private static Logger LOG = LoggerFactory.getLogger(MyJoinSolution.class);
  private static final long TimeOutMs = 10_000;
  
  public static void main(String[] args) throws Exception {
    final var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    
    DataStream<Product> products = env.fromCollection(
        ProductGenerator.generateProductsWithUpdates(5, 2, 2000).iterator(),
        ProductGenerator.ProductWithMetadata.class
      )
      .map(ProductGenerator.ProductWithMetadata::getProduct)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .<Product>forBoundedOutOfOrderness(Duration.ofSeconds(2))
          .withTimestampAssigner((product, timestamp) -> product.getUpdateTime())
      );
    
    DataStream<Order> orders = env.fromCollection(
        OrderGenerator.generateOrdersWithDelay(30, 10, 3000, false).iterator(),
        Order.class
      )
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.
          <Order>forBoundedOutOfOrderness(Duration.ofSeconds(2)).
          withTimestampAssigner((order, timestamp) -> order.getTimestamp())
      );
    
    products.keyBy(Product::getProductId).connect(orders.keyBy(Order::getProductId))
      .process(new MyBufferedJoinFunction(TimeOutMs)).print();
    
    env.execute("Buffered Join Solution");
  }
  
  public static class MyBufferedJoinFunction
    extends KeyedCoProcessFunction<String, Product, Order, JoinExercise.JoinResult> {
    
    private final long timeoutMs;
    
    private transient MapState<String, Product> productBuffer;
    
    private transient MapState<String, Order> orderBuffer;
    
    private transient ValueState<Long> productTimerState;
    
    private transient ValueState<Long> orderTimerState;
    
    public MyBufferedJoinFunction(long timeoutMs) {
      this.timeoutMs = timeoutMs;
    }
    
    @Override
    public void open(OpenContext openContext) throws Exception {
      MapStateDescriptor<String, Product> productBufferDescriptor = new MapStateDescriptor<>(
        "product-buffer",
        TypeInformation.of(String.class),
        TypeInformation.of(Product.class)
      );
      productBuffer = getRuntimeContext().getMapState(productBufferDescriptor);
      
      MapStateDescriptor<String, Order> orderBufferDescriptor = new MapStateDescriptor<String, Order>(
        "order-buffer",
        TypeInformation.of(String.class),
        TypeInformation.of(Order.class)
      );
      orderBuffer = getRuntimeContext().getMapState(orderBufferDescriptor);
      
      var productTimerDescriptor = new ValueStateDescriptor<Long>(
        "product-timer",
        TypeInformation.of(Long.class)
      );
      productTimerState = getRuntimeContext().getState(productTimerDescriptor);
      
      var orderTimerDescriptor = new ValueStateDescriptor<Long>(
        "order-timer",
        TypeInformation.of(Long.class)
      );
      orderTimerState = getRuntimeContext().getState(orderTimerDescriptor);
    }
  }
  
  
}





















