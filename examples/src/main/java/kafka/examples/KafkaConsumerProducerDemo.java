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

import java.util.ArrayList;

public class KafkaConsumerProducerDemo {
    public static void main(String[] args) throws InterruptedException
    {
      String topic = args[1];
      int size = Integer.parseInt(args[2]);
      int count = Integer.parseInt(args[3]);
      int minbytes = Integer.parseInt(args[4]);
      int port = Integer.parseInt(args[5]);
      String mode = args[0];
      if (mode.equals("consume")) {
        while (true) {
          Consumer consumerThread = new Consumer(topic, size, count, minbytes, port);
          consumerThread.start();
          consumerThread.awaitShutdown();
        }
      } else if (mode.equals("produce")) {
        Producer producerThread = new Producer(topic, true, size, count, minbytes, port);
        producerThread.start();
      } else if (mode.equals("multiread")) {
        int partitions = Integer.parseInt(args[6]);
        while (true) {
          ArrayList<GroupConsumer> consumers = new ArrayList<>();
          long l = System.currentTimeMillis();
          for (int i = 0; i < partitions; i++) {
            GroupConsumer consumerThread = new GroupConsumer(topic, size, count, minbytes, port, l, i);
            consumers.add(consumerThread);
          }
          long start = System.currentTimeMillis();
          for (GroupConsumer c : consumers) {
            c.start();
          }
          long sum ;
          long duration;
          long previousSum = 0;
          long previousMoment = start;
          do {
            Thread.sleep(1000);
            sum = 0;
            for (GroupConsumer c : consumers) {
              sum += c.getRecordsTotal();
            }
            long l1 = System.currentTimeMillis();
            duration = l1 - previousMoment;
            System.out.println(String.format("time %d rs %d rs/s %.0f", duration, sum,  (sum - previousSum)/(duration/1000.0)));
            previousSum = sum;
            previousMoment = l1;
          } while (sum < count);
          {
            for (GroupConsumer c : consumers) {
              c.shutdown();
            }
          }
        }
      }
    }
}
