package dev.irontools.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.List;

/**
 * This example demonstrates Flink's serialization requirement.
 * <p>
 * Flink needs to serialize all user functions to distribute them across
 * the cluster. This example will FAIL at runtime because it uses a
 * non-serializable object in a transformation.
 */
public class NonSerializableExample {
    
    /**
     * A non-serializable class that doesn't implement Serializable.
     * This simulates external resources like database connections,
     * file handlers, or complex objects.
     */
    static class PriceCalculator implements Serializable {
        private final double taxRate;
        private final String region;
        
        public PriceCalculator(double taxRate, String region) {
            this.taxRate = taxRate;
            this.region = region;
        }
        
        public double calculateTotal(double amount) {
            return amount * (1 + taxRate);
        }
        
        public String getRegion() {
            return region;
        }
    }
    
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        List<Order> orders = OrderGenerator.generateOrders(10);
        
        // Create a non-serializable object
        PriceCalculator calculator = new PriceCalculator(0.08, "US-CA");
        
        // This will fail at runtime with a NotSerializableException
        // because the lambda captures the non-serializable 'calculator' object
        env.fromData(orders)
            .map(order -> {
                double totalWithTax = calculator.calculateTotal(order.getAmount());
                return String.format("Order %s: $%.2f (with tax in %s: $%.2f)",
                    order.getOrderId(),
                    order.getAmount(),
                    calculator.getRegion(),
                    totalWithTax);
            })
            .print();
        
        env.execute("Non-Serializable Example - Will Fail!");
    }
}
