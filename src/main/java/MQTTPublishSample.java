

import java.util.Random;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MQTTPublishSample {

    public static void main(String[] args) {
    	Random r=new Random();
        String topic        = "Spark/wordcount";
        String[] content      = {"Message from MqttPublishSample","message is delivered to mqtt broker","mqtt broker is connected"};
        int qos             = 2;
        String broker       = "tcp://52.73.161.142:1883";
        String clientId     = "devices".concat(String.valueOf((new Random()).nextInt(9999)));
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
          
            System.out.println("Connecting to broker: "+broker);
            sampleClient.connect(connOpts);
            System.out.println("Connected");
          
            MqttMessage message = new MqttMessage(content[r.nextInt(content.length)].getBytes());
            message.setQos(qos);
          
            for(;;)
            {
            	message.setPayload(content[r.nextInt(content.length)].getBytes());
            sampleClient.publish(topic, message);
         
			Thread.sleep(5000);
	
            
            }
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}