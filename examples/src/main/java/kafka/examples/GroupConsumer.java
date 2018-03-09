/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class GroupConsumer extends ShutdownableThread
{
  private final KafkaConsumer<Integer, byte[]> consumer;
  private final String topic;
  private final int size;
  private final int count;
  private final int minbytes;
  private final int port;
  private volatile long recordsTotal = 0;

  public GroupConsumer(String topic, int size, int count, int minbytes, int port, long id, int partition)
  {
    super("KafkaConsumerExample", false);
    this.size = size;
    this.count = count;
    this.minbytes = minbytes;
    this.port = port;
    this.partition = partition;
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + port);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer" + id);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Integer.MAX_VALUE));
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE-1));
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, minbytes + "");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE +  "");
    File file = new File("test.properties");
    if (file.exists()) {
      try {
        props.load(new FileInputStream(file));
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }

    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.IntegerDeserializer"
    );
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    );

    consumer = new KafkaConsumer<>(props);
    consumer.assign(Collections.singletonList(new TopicPartition(topic, this.partition)));
    this.topic = topic;
  }

  long timeSum = 0;
  int calCount = 0;
  int step = 0;
  long firstOffset = -1;

  private final int partition;

  @Override
  public void doWork()
  {
    long l = System.currentTimeMillis();
    ConsumerRecords<Integer, byte[]> records = consumer.poll(5000);
    ConsumerRecord<Integer, byte[]> next = null;
    if (records.iterator().hasNext()) {
      next = records.iterator().next();
    }
    if (firstOffset == -1) {
      firstOffset = next.offset();
    }

    long time = System.currentTimeMillis() - l;

    if (calCount > 2) {
      timeSum += time;
      step++;
    }
    boolean batch = Boolean.getBoolean("batch");
    long count = records.count();
    recordsTotal += count;
    StringBuilder stringBuilder = new StringBuilder("p " + partition + " ");
    stringBuilder.append("topic " + topic + " batch " + batch
                     + " time " + time + " rs " + count //+ " total " + sum
    );
    if (batch) {
      stringBuilder.append(" lo " + records.lastOffset());
    } else {
      if (next != null)
        stringBuilder.append(" sz " + next.value().length + " fo " + next.offset());

      long last = 0;
      for (ConsumerRecord r : records) {
        last = r.offset();
      }
      stringBuilder.append(" lo " + last);

    }
    if (calCount > 2) {
      stringBuilder.append(" total (ms) " + timeSum);
      stringBuilder.append(" avg (ms) " + timeSum / step);
    }
    System.out.println(stringBuilder);

    calCount++;
//    if (recordsTotal >= this.count) {
//      initiateShutdown();
//    }
  }

  @Override
  public String name()
  {
    return null;
  }

  @Override
  public boolean isInterruptible()
  {
    return false;
  }

  public long getRecordsTotal()
  {
    return recordsTotal;
  }
}
