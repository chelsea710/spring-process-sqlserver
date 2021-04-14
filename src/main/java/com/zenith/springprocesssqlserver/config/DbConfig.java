package com.zenith.springprocesssqlserver.config;

import cn.hutool.core.util.StrUtil;
import com.jfinal.kit.PropKit;
import com.jfinal.plugin.activerecord.ActiveRecordPlugin;
import com.jfinal.plugin.activerecord.CaseInsensitiveContainerFactory;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.jfinal.plugin.activerecord.dialect.PostgreSqlDialect;
import com.jfinal.plugin.activerecord.dialect.SqlServerDialect;
import com.jfinal.plugin.druid.DruidPlugin;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author LHR
 * @date 20210326
 */
@Configuration
public class DbConfig {

    @Value("${jfinal.sqlserver.datasource.url}")
    private String url;

    @Value("${jfinal.sqlserver.datasource.class}")
    private String driverClass;

    @Value("${jfinal.sqlserver.datasource.user}")
    private String username;

    @Value("${jfinal.sqlserver.datasource.password}")
    private String password;


    @Value("${jfinal.datasource.url}")
    private String pgUrl;

    @Value("${jfinal.datasource.class}")
    private String pgDriverClass;

    @Value("${jfinal.datasource.user}")
    private String pgUsername;

    @Value("${jfinal.datasource.password}")
    private String pgPassword;


    @Bean
    public ActiveRecordPlugin initActiveRecirdPlugin(){
        //sqlserver数据库
        DruidPlugin druidPlugin = new DruidPlugin(/*"jdbc:sqlserver://23.8.15.49:1433;Database=区县02涪陵区"*/url, username, password.trim());
        druidPlugin.setDriverClass(driverClass); //Users是你的数据库中的某一个表
        // 配置ActiveRecord插件
        ActiveRecordPlugin arp = new ActiveRecordPlugin(DBConstant.SQLSERVER,druidPlugin);
        // 配置Sqlserver方言
        arp.setDialect(new SqlServerDialect());
        arp.setShowSql(true);
        // 配置属性名(字段名)大小写不敏感容器工厂
        arp.setContainerFactory(new CaseInsensitiveContainerFactory());
        druidPlugin.start();
        arp.start();
        return arp;
    }

    @Bean
    public ActiveRecordPlugin initPGActiveRecirdPlugin(){
        //sqlserver数据库
        DruidPlugin druidPlugin = new DruidPlugin(pgUrl, pgUsername, pgPassword.trim());
        druidPlugin.setDriverClass(pgDriverClass); //Users是你的数据库中的某一个表
        // 配置ActiveRecord插件
        ActiveRecordPlugin arp = new ActiveRecordPlugin(DBConstant.PG,druidPlugin);
        // 配置Sqlserver方言
        arp.setDialect(new PostgreSqlDialect());
        arp.setShowSql(true);
        // 配置属性名(字段名)大小写不敏感容器工厂
        arp.setContainerFactory(new CaseInsensitiveContainerFactory());
        druidPlugin.start();
        arp.start();
        return arp;
    }

}
