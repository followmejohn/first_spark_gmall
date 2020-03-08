package com.john.dw.gmallpublisher.service;

import com.alibaba.fastjson.JSON;
import com.john.dw.gmallpublisher.mapper.DauMapper;
import com.john.dw.gmallpublisher.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Service
public class PublisherServiceImp implements PublisherService {
    @Autowired
    DauMapper dauMapper;
    @Override
    public Long getDau(String date){
        // 从数据层读取数据, 然后给Controller使用
        return dauMapper.getDau(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map> hourDau = dauMapper.getHourDau(date);
//        System.out.println(JSON.toJSONString(hourDau));
        Map<String, Long> map = new HashMap<>();
        for (Map mp : hourDau) {
            String key = (String)mp.get("LOGHOUR");
            Long value = (Long)mp.get("COUNT");
            map.put(key,value);
        }
        return map;
    }
    @Autowired
    OrderMapper orderMapper;
    @Override
    public Double getTotalAmount(String date) {
        Double total = orderMapper.getTotalAmount(date);
        return total == null ? 0 : total;
    }

    @Override
    public Map<String, Double> getHourAmount(String date) {
        List<Map> hourAmount = orderMapper.getHourAmount(date);
        Map<String, Double> map = new HashMap<>();
        for(Map mp : hourAmount){
            String key = (String)mp.get("CREATE_HOUR");
            Double value = ((BigDecimal) mp.get("SUM")).doubleValue();
            map.put(key, value);
        }
        return map;
    }
}
