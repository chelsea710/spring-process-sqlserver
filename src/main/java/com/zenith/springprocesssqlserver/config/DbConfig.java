package com.zenith.springprocesssqlserver.config;

import com.jfinal.plugin.activerecord.ActiveRecordPlugin;
import com.jfinal.plugin.activerecord.CaseInsensitiveContainerFactory;
import com.jfinal.plugin.activerecord.dialect.PostgreSqlDialect;
import com.jfinal.plugin.druid.DruidPlugin;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

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

    @Value("${jfinal.datasource.olap.url}")
    private String pgOlapUrl;


//    @Bean
//    public ActiveRecordPlugin initActiveRecirdPlugin(){
//        //sqlserver数据库
//        DruidPlugin druidPlugin = new DruidPlugin(/*"jdbc:sqlserver://23.130.10.202:1433;Database=区县02涪陵区"*/url, username, password.trim());
//        druidPlugin.setDriverClass(driverClass); //Users是你的数据库中的某一个表
//        // 配置ActiveRecord插件
//        ActiveRecordPlugin arp = new ActiveRecordPlugin(DBConstant.SQLSERVER,druidPlugin);
//        // 配置Sqlserver方言
//        arp.setDialect(new SqlServerDialect());
//        arp.setShowSql(true);
//        // 配置属性名(字段名)大小写不敏感容器工厂
//        arp.setContainerFactory(new CaseInsensitiveContainerFactory());
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }

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

    @Bean
    public ActiveRecordPlugin initPGActiveRecirdOlapPlugin(){
        //sqlserver数据库
        DruidPlugin druidPlugin = new DruidPlugin(pgOlapUrl, pgUsername, pgPassword.trim());
        druidPlugin.setDriverClass(pgDriverClass); //Users是你的数据库中的某一个表
        // 配置ActiveRecord插件
        ActiveRecordPlugin arp = new ActiveRecordPlugin(DBConstant.OLAP,druidPlugin);
        // 配置Sqlserver方言
        arp.setDialect(new PostgreSqlDialect());
        arp.setShowSql(true);
        // 配置属性名(字段名)大小写不敏感容器工厂
        arp.setContainerFactory(new CaseInsensitiveContainerFactory());
        druidPlugin.start();
        arp.start();
        return arp;
    }


//    @Bean
//    public ActiveRecordPlugin initPGOlapActiveRecirdPlugin(){
//        //sqlserver数据库
//        DruidPlugin druidPlugin = new DruidPlugin("jdbc:postgresql://23.133.10.200:5432/olap", pgUsername, pgPassword.trim());
//        druidPlugin.setDriverClass("org.postgresql.Driver"); //Users是你的数据库中的某一个表
//        // 配置ActiveRecord插件
//        ActiveRecordPlugin arp = new ActiveRecordPlugin("olap",druidPlugin);
//        // 配置Sqlserver方言
//        arp.setDialect(new PostgreSqlDialect());
//        arp.setShowSql(true);
//        // 配置属性名(字段名)大小写不敏感容器工厂
//        arp.setContainerFactory(new CaseInsensitiveContainerFactory());
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }

//    @Bean
//    public ActiveRecordPlugin initPGActiveLiangjiangGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_liangjiang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveLiangjiangOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_liangjiang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveBannaGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_banan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivebananOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_banan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivebeibeiGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_beibei");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivebeibeiOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_beibei");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivebishanGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_bishan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivebishanOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_bishan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivechangshouGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_changshou");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivechangshouOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_changshou");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivechengkouGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_chengkou");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivechengkouOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_chengkou");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivedadukouGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_dadukou");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivedadukouOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_dadukou");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivedazuGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_dazu");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivedazuOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_dazu");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivedianjiangGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_dianjiang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivedianjiangOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_dianjiang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivefengduGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_fengdu");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivefengduOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_fengdu");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivefengjieGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_fengjie");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivefengjieOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_fengjie");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivefulingGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_fuling");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivefulingOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_fuling");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivegaoxinGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_gaoxin");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivegaoxinOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_gaoxin");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivehechuanGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_hechuan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivehechuanOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_hechuan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejianchayuanGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_jianchayuan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejianchayuanOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_jianchayuan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejiangbeiGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_jiangbei");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejiangbeiOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_jiangbei");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejiangjinGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_jiangjin");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejiangjinOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_jiangjin");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejianyujuGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_jianyuju");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejianyujuOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_jianyuju");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejiaoyujiaozhiGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_jiaoyujiaozhi");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejiaoyujiaozhiOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_jiaoyujiaozhi");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejiulongpoGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_jiulongpo");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivejiulongpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_jiulongpo");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveakaizhouGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_kaizhou");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivekaizhoupoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_kaizhou");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivealiangpingGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_liangping");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveliangpingpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_liangping");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveanananGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_nanan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivenananpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_nanan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveananchuanGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_nanchuan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivenanchuanpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_nanchuan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveapengshuiGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_pengshui");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivepengshuipoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_pengshui");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveaqianjiangGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_qianjiang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveqianjiangpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_qianjiang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveaqijiangGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_qijiang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveqijiangpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_qijiang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivearongchangGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_rongchang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiverongchangpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_rongchang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveashapingbaGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_shapingba");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveshapingbapoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_shapingba");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveashichangjianduGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_shichangjiandu");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveshichangjiandupoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_shichangjiandu");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveashizhuGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_shizhu");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveshizhupoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_shizhu");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveatongliangGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_tongliang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivetongliangpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_tongliang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveatongnanGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_tongnan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivetongnanpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_tongnan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveatongzhanGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_gonganju");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivetongzhanpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_gonganju");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveawanshengGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_wansheng");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivewanshengpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_wansheng");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveawanzhouGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_wanzhou");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivewanzhoupoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_wanzhou");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveawulongGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_wulong");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivewulongpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_wulong");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveawushanGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_wushan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivewushanpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_wushan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveawuxiGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_wuxi");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivewuxipoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_wuxi");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveaxishanGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_xiushan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivexishanpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_xiushan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveayongchuanGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_yongchuan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveyongchuanpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_yongchuan");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveayouyangGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_youyang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveyouyangpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_youyang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveayubeiGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_yubei");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveyubeipoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_yubei");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveayunyangGbPlugin(){
//            List<Object> obj = this.processDataSource("gb_yunyang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveyunyangpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_yunyang");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveazongxianGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_zongxian");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActivezongxianpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_zongxian");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveayuzhongGbPlugin(){
//        List<Object> obj = this.processDataSource("gb_yuzhong");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }
//
//    @Bean
//    public ActiveRecordPlugin initPGActiveyuzhongpoOlapPlugin(){
//        List<Object> obj = this.processDataSource("olap_yuzhong");
//        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
//        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
//        druidPlugin.start();
//        arp.start();
//        return arp;
//    }

    @Bean
    public ActiveRecordPlugin initPGActiveaproGbPlugin(){
        List<Object> obj = this.processDataSource("gb_2020_pro");
        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
        druidPlugin.start();
        arp.start();
        return arp;
    }

    @Bean
    public ActiveRecordPlugin initPGActiveolapProoOlapPlugin(){
        List<Object> obj = this.processDataSource("olap_2021_pro");
        DruidPlugin druidPlugin = (DruidPlugin)obj.get(0);
        ActiveRecordPlugin arp = (ActiveRecordPlugin)obj.get(1);
        druidPlugin.start();
        arp.start();
        return arp;
    }



    public List<Object> processDataSource(String areaName){
        List<Object> result = new ArrayList<>();
        DruidPlugin druidPlugin = new DruidPlugin("jdbc:postgresql://127.0.0.1:5432/"+areaName, "postgres", "20191809");
        druidPlugin.setDriverClass("org.postgresql.Driver"); //Users是你的数据库中的某一个表
        // 配置ActiveRecord插件
        ActiveRecordPlugin arp = new ActiveRecordPlugin(areaName,druidPlugin);
        // 配置Sqlserver方言
        arp.setDialect(new PostgreSqlDialect());
        arp.setShowSql(true);
        // 配置属性名(字段名)大小写不敏感容器工厂
        arp.setContainerFactory(new CaseInsensitiveContainerFactory());
        result.add(druidPlugin);
        result.add(arp);
        return result;
    }



}
