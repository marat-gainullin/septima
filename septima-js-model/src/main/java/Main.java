import com.septima.entities.SqlEntities;
import com.septima.sample.OrdersModel;

import java.util.Map;
import java.util.function.Function;

public class Main {

    private static final SqlEntities ENTITIES = new SqlEntities(null, "septima");

    public static void main(String[] args) {
        OrdersModel model = new OrdersModel(ENTITIES);
        model.requestCustomers(Map.of())
                .thenApply(customers -> {
                            customers.values().forEach(customer -> {
                            });
                            return model.save();
                        }
                ).thenCompose(Function.identity())
                .thenAccept(v -> {
                });
    }
}
