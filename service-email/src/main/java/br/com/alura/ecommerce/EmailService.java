package br.com.alura.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;

public class EmailService implements ConsumerService<String>{

    private static final int THREADS = 5;

	public static void main(String[] args) throws ExecutionException, InterruptedException   {
    	new ServiceRunner(EmailService::new).start(THREADS);
    }

	@Override
	public String getTopic() {
		return  "ECOMMERCE_SEND_EMAIL";
	}
	
	@Override
	public String getConsumerGroup() {
		return EmailService.class.getSimpleName();
	}

	@Override
	public void parse(ConsumerRecord<String, Message<String>> record) {
		 System.out.println("------------------------------------------");
	        System.out.println("Send email");
	        System.out.println(record.key());
	        System.out.println(record.value().getPayload());
	        System.out.println(record.partition());
	        System.out.println(record.offset());
	        try {
	            Thread.sleep(1000);
	        } catch (InterruptedException e) {
	            // ignoring
	            e.printStackTrace();
	        }
	        System.out.println("Email sent");
	}

}
