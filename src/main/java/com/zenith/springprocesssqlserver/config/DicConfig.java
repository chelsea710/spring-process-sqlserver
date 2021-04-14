package com.zenith.springprocesssqlserver.config;

import cn.hutool.core.util.StrUtil;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author LHR
 * @date 2021/4/10
 */
@Configuration
@AutoConfigureBefore(DbConfig.class)
public class DicConfig {

    @Bean(name = "getDicMap")
    public Map<String,List<Record>> getDicMap(){
        return Db.use(DBConstant.PG).find("select * from \"code_value\"").stream()
                .filter(var->StrUtil.isNotEmpty(var.getStr("CODE_TYPE")))
                .collect(Collectors.groupingBy(var->var.getStr("CODE_TYPE")));
    }
}
