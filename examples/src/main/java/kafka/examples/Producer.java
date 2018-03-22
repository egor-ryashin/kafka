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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class Producer extends Thread {

    private final String topic;
    private final Boolean isAsync;
    private final int size;
    private final int count;
    private Properties props;
    private static Boolean stringdata = Boolean.getBoolean("stringdata");

    public Producer(String topic, Boolean isAsync, int size, int count, int minbytes, int port) {
        this.size = size;
        this.count = count;
        props = new Properties();
        props.put("bootstrap.servers", "localhost:" + port);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        if (stringdata) {
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        } else {
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        }
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "300000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "300000");
        props.put("buffer.memory", minbytes + "");
        props.put("max.request.size", Integer.MAX_VALUE + "");


        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        Boolean stringdata = Boolean.getBoolean("stringdata");
        if (stringdata) {
            produce(new KafkaProducer<Integer, String>(props));
        } else {
            produce(new KafkaProducer<Integer, byte[]>(props));
        }
    }

    private <T> void produce(KafkaProducer<Integer, T> producer) {
        int messageNo = 1;
        int msgCount = count;
        Object value ;
        if (stringdata) {
            value = "stringstring";
        } else {
            byte[] bytes = new byte[size];

            for (int i = 0; i < bytes.length; i += bytes.length / 10) {
                bytes[i] = (byte) (Math.random() * 100);
            }
            value = bytes;
        }


        System.out.println("size " + size + " msgs " + msgCount);
        for (int i = 0; i < msgCount; i++) {
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<Integer, T>(topic, (T)value));
                if (i % 10000 == 0) {
                    System.out.println(new Date() + " Sent message: (" + messageNo + ", " + i + ")");
                }
            }
            ++messageNo;
        }
        try {
            System.out.println("Posted " + ((stringdata ? ((String)value).length() : ((byte[])value).length) * msgCount));
            Thread.sleep(15000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
