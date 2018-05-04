package org.experimental.cafenea;

import org.experimental.Env;
import org.experimental.MessageBus;
import org.experimental.runtime.EndpointWire;
import org.testng.annotations.Test;

import java.util.UUID;


/*

customer
	send(NewOrder -> [barsita])

	on PaymentDue
		reply(SubmitPayment)
	on DrinkReady
		end.

cashier
	on NewOrder
		Publish(PrepareDrink)
		Reply(PaymentDue)

	on SubmitPayment
		Publish(PaymentComplete)

barista
	on PrepareDrink
		publish(DrinkReady)
	on PaymentComplete
		publish(DrinkReady)

 */


public class SampleApp extends Env {

    ////////// barista
    public class DrinkReady
    {
        public DrinkReady(UUID correlationId, String drink) {
            this.correlationId = correlationId;
            this.drink = drink;
        }

        public UUID correlationId;
        public String drink;
    }

    public class BaristaSaga{

        private String drink;
        private UUID correlationId;
        private boolean drinkIsReady;
        private boolean gotPayment;

        public void handle(MessageBus bus, PrepareDrink message)
        {
            drink = message.drinkName;
            correlationId = message.correlationId;

            for (int i = 0; i < 10; i++)
            {
                System.out.println("Barista: preparing drink: " + drink);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            drinkIsReady = true;
            SubmitOrderIfDone(bus);
        }

        public void handle(MessageBus bus, PaymentComplete message)
        {
            System.out.println("Barista: got payment notification");
            gotPayment = true;
            SubmitOrderIfDone(bus);
        }

        private void SubmitOrderIfDone(MessageBus bus)
        {
            if (gotPayment && drinkIsReady)
            {
                System.out.println("Barista: drink is ready");
                bus.publish(new DrinkReady(correlationId, drink));
            }
        }
    }

    ////////// cashier
    public class NewOrder
    {
        public NewOrder(String drinkName, int size, String customerName) {
            this.drinkName = drinkName;
            this.size = size;
            this.customerName = customerName;
        }

        public String drinkName;
        public int size ;
        public String customerName;
    }
    public class PaymentDue
    {
        public PaymentDue(String customerName, UUID transactionId, double amount) {
            this.customerName = customerName;
            this.transactionId = transactionId;
            this.amount = amount;
        }

        public String customerName;
        public UUID transactionId;
        public double amount;
    }
    public class SubmitPayment
    {
        public SubmitPayment(UUID correlationId, double amount) {
            this.correlationId = correlationId;
            this.amount = amount;
        }

        public UUID correlationId;
        public double amount;
    }
    public class PaymentComplete
    {
        public PaymentComplete(UUID correlationId) {
            this.correlationId = correlationId;
        }

        public UUID correlationId;
    }

    public class PrepareDrink
    {
        public String drinkName;
        public int size;
        public String customerName;
        public UUID correlationId;

        public PrepareDrink(String drinkName, int size, String customerName, UUID correlationId) {
            this.drinkName = drinkName;
            this.size = size;
            this.customerName = customerName;
            this.correlationId = correlationId;
        }
    }

    public class CashierSaga{
        public void handle(MessageBus bus, NewOrder message){
            System.out.println("Cashier: got new order");
            UUID correlationId = UUID.randomUUID();

            bus.publish(new PrepareDrink(
                message.drinkName,
                message.size,
                message.customerName,
                correlationId
            ));

            bus.reply(new PaymentDue(
                    message.customerName,
                    correlationId,
                    message.size * 1.25
            ));
        }

        public void handle(MessageBus bus, SubmitPayment message){
            System.out.println("Cashier: got payment");
            bus.publish(new PaymentComplete(message.correlationId));
        }
    }


    ////////// customer
    public class CustomerController {

        public void handle(MessageBus bus, PaymentDue message) {
            bus.reply(new SubmitPayment(
                    message.transactionId,
                    message.amount)
            );
        }

        public void handle(DrinkReady message) {
            System.out.println("yay here is my latte");
        }

        public void buyMeADrink(MessageBus bus) {
            bus.send(new NewOrder("latte", 3, "ruslan"));
        }
    }

    @Test
    public void end2end() throws Exception {

        String kfk = CLUSTER.getKafkaConnect();
        String zk = CLUSTER.getZookeeperString();
        try(
                EndpointWire cashier = new EndpointWire("cashier", kfk, zk);
                EndpointWire barista = new EndpointWire("barista", kfk, zk);
                EndpointWire customer = new EndpointWire("customer", kfk, zk)
        ){
            CashierSaga cashierSaga = new CashierSaga();
            cashier.registerHandler(NewOrder.class, b -> m -> cashierSaga.handle(b, m));
            cashier.registerHandler(SubmitPayment.class, b -> m -> cashierSaga.handle(b, m));
            cashier.configure();

            BaristaSaga baristaSaga = new BaristaSaga();
            barista.subscribeToEndpoint("cashier", PrepareDrink.class, PaymentComplete.class);
            barista.registerHandler(PrepareDrink.class, b -> m -> baristaSaga.handle(b, m));
            barista.registerHandler(PaymentComplete.class, b -> m -> baristaSaga.handle(b, m));
            barista.configure();

            CustomerController controller = new CustomerController();
            customer.registerEndpointRoute("cashier", NewOrder.class);
            customer.subscribeToEndpoint("barista", DrinkReady.class);
            customer.registerHandler(DrinkReady.class, b -> controller::handle);
            customer.registerHandler(PaymentDue.class, b -> m -> controller.handle(b, m));
            customer.configure();

            Thread.sleep(500);

            controller.buyMeADrink(customer.getMessageBus());

            Thread.sleep(8000);
        }
    }
}
