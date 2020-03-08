package com.john.dw.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.john.dw.gmall.common.Constant;

import java.util.Properties;

@RestController
public class LoggerController {
    @PostMapping("/log")
    public String dolog(@RequestParam("log") String log){
        //1.把日志加一个时间戳
        log = addTS(log);
        //2.把日志存入到文件（将来别的程序可通过flume从这采集数据）
        save2File(log);
        //3.把日志写入kafka
        save2Kafka(log);
        System.out.println(log);
        return "haha";
    }

    @Autowired
    KafkaTemplate<String, String> kafka;
    private KafkaProducer<String, String> getProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092");
        // key的序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(props);
    }
    KafkaProducer<String, String> p = getProducer();
    private void save2Kafka(String log) {
        String topic = Constant.EVENT_TOPIC;
        if(log.contains("startup")){
            topic = Constant.STARTUP_TOPIC;
        }
//        kafka.send(topic,log);
        p.send(new ProducerRecord<>(topic, log));
    }

    Logger logger = LoggerFactory.getLogger(LoggerController.class);
    private void save2File(String log) {
        logger.info(log);
    }

    private String addTS(String log) {
        JSONObject jsonObj = JSON.parseObject(log);
        jsonObj.put("ts",System.currentTimeMillis());
        return jsonObj.toJSONString();
    }
}
