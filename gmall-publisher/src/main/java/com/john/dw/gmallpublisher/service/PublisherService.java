package com.john.dw.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    // 获取日活的接口
    Long getDau(String date);
    //每小时的
    Map<String, Long> getHourDau(String date);
    //获取总销售额
    Double getTotalAmount(String date);
    //获取每小时销售额
    Map<String, Double> getHourAmount(String date);
}
 /*
        hour: 10点  count: 100
        hour: 11点 count: 110
        hour: 12点 count: 120
        ...

        每行用 Map
        多行用List把每行封装起来

        List<Map> => Map<String, Long>

        10点 :100
        11点 : 110
        12点 :120
     */