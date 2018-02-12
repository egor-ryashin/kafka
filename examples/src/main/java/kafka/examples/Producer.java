/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import scala.Console;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer extends Thread {
    private final KafkaProducer<Integer, byte[]> producer;
    private final String topic;
    private final Boolean isAsync;
    private final int size;

    public Producer(String topic, Boolean isAsync, int size) {
        this.size = size;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "300000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "300000");
//        props.put("batch.size", "1");
        props.put("buffer.memory", 2000_000_000l + "");
        props.put("max.request.size", Integer.MAX_VALUE + "");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        int messageNo = 1;
        long totalBytes = 5_000_000_000l;
        int msgCount = (int) (totalBytes / size);
        byte[] bytes = new byte[size];

//        for (int i = 0; i < bytes.length; i++) {
//            if (i % (bytes.length == 0)
//              bytes[i] = (byte) (Math.random() * 100);
//        }
      for ( int i =0; i < bytes.length; i+= bytes.length/100)
        bytes[i] = (byte) (Math.random() * 100);

        System.out.println("size " + size + " msgs " + msgCount);
        for (int i = 0; i < msgCount; i++)  {
//            String messageStr = "Message_" + messageNo;
//            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<Integer, byte[]>(topic, bytes));
                if (i % 10000 == 0) {
                    System.out.println(new Date() + " Sent message: (" + messageNo + ", " + i + ")");
                }
            } else { // Send synchronously
//                try {
//                    producer.send(new ProducerRecord<>(topic,
//                        messageNo,
//                        messageStr)).get();
//                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
//                } catch (InterruptedException | ExecutionException e) {
//                    e.printStackTrace();
//                }
            }
            ++messageNo;
//            Console.readLine();
        }
        try {
          System.out.println("Posted " + (bytes.length * msgCount));
            Thread.sleep(15000000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
//        long elapsedTime = System.currentTimeMillis() - startTime;
//        if (metadata != null) {
//            System.out.println(
//                "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
//                    "), " +
//                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
//        } else {
//            exception.printStackTrace();
//        }
    }
}
