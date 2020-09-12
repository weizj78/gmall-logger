package xyz.weizj.gmalllogger.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import constans.GmallConstans;

@Slf4j
@Controller
public class LogController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;



    @ResponseBody
    @RequestMapping("/log")
    public String LogRecever(@RequestParam("logString") String str){

        JSONObject jsonObject = JSONObject.parseObject(str);
        jsonObject.put("ts",System.currentTimeMillis());

        log.info(jsonObject.toJSONString());

        if("startup".equals(jsonObject.getString("type")))
            kafkaTemplate.send(GmallConstans.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        else
            kafkaTemplate.send(GmallConstans.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());

        return "success";
    }
}
