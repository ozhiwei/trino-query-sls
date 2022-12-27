package com.github.trino.querysls;

import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Utils {

  public static Producer createProducer(String project, String endpoint, String accessKeyId, String accessKeySecret) {
    ProducerConfig producerConfig = new ProducerConfig();
    Producer producer = new LogProducer(producerConfig);

    ProjectConfig projectConfig = new ProjectConfig(project, endpoint, accessKeyId, accessKeySecret);
    producer.putProjectConfig(projectConfig);


    return producer;
  }


  static Map<String, String> getConfig() throws IOException {
    Properties properties = new Properties();
    String configPath = "config/event-listener.properties";
    properties.load(new FileInputStream(configPath));


    Map<String, String> config = (Map) properties;

    return config;
  }

  public static boolean parseBoolean(String s) {
    return s != null && !s.equalsIgnoreCase("false");
  }

}
