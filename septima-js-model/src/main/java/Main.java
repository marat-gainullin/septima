import java.util.Map;

public class Main {

    public static void main(String[] args) {
        OrdersModel model = new OrdersModel();
        model.requestCustomers(Map.of())
                .thenAccept(customers -> customers.values().forEach(customer -> {
                        })
                );
    }
}
