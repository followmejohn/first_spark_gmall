package com.john.dw.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //查询日活总数
    // 方法内部到底执行什么样的sql, 需要去写xml文件, 在xml文件中定义sql语句
    Long getDau(String date);
    //查询小时
    List<Map> getHourDau(String date);
}
