package com.zenith.springprocesssqlserver.sync.dao;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Page;
import com.jfinal.plugin.activerecord.Record;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author LHR
 * @date 20210326
 */
@Repository
public class SyncDao {

    public List<Record> findSqlServerA01(){
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[a01]");
    }

    public List<Record> findSqlServerA01Test(){
        return Db.use(DBConstant.SQLSERVER).find("select * from [test_dev].[gb].[a01] where a0184 is not null and a0184 <> ''");
    }

    public List<Record> findSqlServerA44(){
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[a44] order by startdatetime");
    }

    public Page<Record> findSqlServerPhoto(Integer pageNum, Integer pageSize){
        return Db.use(DBConstant.SQLSERVER).paginateByFullSql(pageNum,pageSize,"select count(*) from [区县02涪陵区].[dbo].[aphoto] ","select * from [区县02涪陵区].[dbo].[aphoto] order by leader_code ");
    }

    public List<Record> findSqlServerA02(){
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[A02] where a0255 = '1' ");
    }

    public List<Record> findSqlServerA05(){
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[A05]  ");
    }


    public List<Record> findSqlServerA08() {
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[a0809]  ");
    }



    public Set<String> getDic(String codeType){
        return Db.use(DBConstant.PG).find("select * from \"code_value\" where \"CODE_TYPE\" = ?",codeType).stream().map(var->var.getStr("CODE_VALUE")).collect(Collectors.toSet());
    }


    public List<Record> findSqlServerAcq01() {
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[acq01]");
    }


    public List<Record> findSqlServerA36() {
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[a36]  ");
    }

    public List<Record> findSqlServerB01() {
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[b01]  ");
    }

    public List<Record> findSqlServerAcq03(){
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[acq03] where acq0302 is not null and acq0302 <> '' order by acq0301 desc,acq0302 desc");
    }

    public Map<String,String> getDicMap(String codeType) {
        return Db.use(DBConstant.PG).find("select * from \"code_value\" where \"CODE_TYPE\" = ?",codeType).stream().collect(Collectors.toMap(key->key.getStr("CODE_VALUE"),value->value.getStr("CODE_NAME"),(key1,key2)->key1));
    }

    public List<Record> findSqlServerA06() {
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[A06]  ");
    }


    public List<Record> findSqlServerA14z2() {
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[a14z2]  ");
    }

    public List<Record> findSqlServerA14z3() {
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[a14z3]  ");
    }

    /**
     * 查询图片文件
     */
    public Set<String> findfindPicTable() {
        return Db.use(DBConstant.PG).find("select * from \"findPicFuling\"").stream().map(var->var.getStr("A0000")).collect(Collectors.toSet());
    }

    /**
     * 查询国企事业单位人员
     * @return 人员数据
     */
    public Set<String> findPgCityAnfCompanyMem() {
        return Db.use(DBConstant.PG).find("select  \"a01\".\"A0000\" from \"a01\" inner join \"a02\" on \"a01\".\"A0000\" = \"a02\".\"A0000\" AND \"a02\".\"A0255\" = '1' " +
                "inner join \"b01\" on \"a02\".\"A0201B\" = \"b01\".\"id\"  and \"b01\".\"isDelete\" = 0 " +
                "where \"b01\".\"B0111\" like '001.003%' or \"b01\".\"B0111\" like '001.030%' group by \"a01\".\"A0000\"").stream().map(var->var.getStr("A0000")).collect(Collectors.toSet());
    }


    /**
     * 查询每个cdc系统表的数据
     * @param tableName 表名称
     * @return count长度
     */
    public Integer findCdcSystemTableCount(String tableName){
        return Db.use(DBConstant.SQLSERVER).queryInt("select count(*) from "+tableName+"");
    }


    /**
     * 查询 所有的cdc表名称
     */
    public List<Record> findCdcTableNameList(){
        return Db.use(DBConstant.PG).find("select * from \"syncSqlserverCdcConfig\"");
    }

    /**
     * 查询每个cdc系统表的数据
     * @param tableName 表名称
     * @return count长度
     */
    public List<Record> findCdcSystemTableList(String tableName){
        return Db.use(DBConstant.SQLSERVER).find("select * from "+tableName+"");
    }


    /**
     * 删除数据源
     * @param table 表名称
     * @param field 字段名称
     * @param value 字段的值
     * @return 成功与否
     */
    public int deletePGDataSource(String table,String field,String value){
        return Db.use(DBConstant.PG).delete("delete from \""+table+"\" where \""+field+"\" = ? ",value);
    }
    /**
     * 查询数据源
     * @param table 表名称
     * @param field 字段名称
     * @param value 字段的值
     * @return 查询数据
     */
    public List<Record> findPgDataSource(String table,String field,String value){
        return Db.use(DBConstant.PG).find("select * from \""+table+"\" where \""+field+"\" = ?",value);
    }

    /**
     * 修改与sqlserver的映射表
     * @param leader_code 主键
     * @param a0184 身份证
     */
    public void updateSyncA0184Mapping(String leader_code, String a0184) {
        Db.use(DBConstant.PG).update("update \"syncArchivesA0184Mapping\" set \"A0184\" = ? where \"A0000\" = ?",a0184,leader_code);
    }

    /**
     * 删除cdc系统表
     * @param startLsnDeleteList
     */
    public void deleteCdcSystemByIds(List<String> startLsnDeleteList,String tableName) {
        if (startLsnDeleteList.size() > 0) {
            String join = CollectionUtil.join(startLsnDeleteList, ",");
            Db.use(DBConstant.SQLSERVER).delete("delete from "+tableName+" where __$start_lsn in ("+join+")");
        }
    }

    /**
     * 查询身份证重复校核
     * @param a0184 身份证
     * @return 重复人员
     */
    public List<Record> findA0184OnlyOne(String a0184) {
        if(StrUtil.isNotEmpty(a0184)){
            return Db.use(DBConstant.PG).find("select \"A0101\",\"A0192\" from \"a01\" where \"A0184\" = ?",a0184);
        }
        return new ArrayList<>();
    }

    /**
     * sqlserver 根据唯一id 查询身份证信息
     * @param leader_code 唯一id
     * @return 身份证
     */
    public String findA0184ByLeaderCode(String leader_code) {
        return Db.use(DBConstant.PG).queryStr("select \"A0184\" from \"syncArchivesA0184Mapping\" where \"A0000\" = ?",leader_code);
    }

    /**
     * 根据人员a0000查询人员的职务信息
     * @param a0000 人员唯一标识
     * @return 人员a02的集合
     */
    public List<Record> findPgA02InfoByA0000(String a0000) {
        return Db.use(DBConstant.PG).find("select * from \"a02\" where \"A0000\" = ? and \"A0255\" = '1'",a0000);
    }

    /**
     * 人员a05详细信息
     * @param a0000 人员唯一标识
     * @return 人员职务职级信息集合
     */
    public List<Record> findPgA05InfoByA0000(String a0000) {
        return Db.use(DBConstant.PG).find("select * from \"a05\" where \"A0000\" = ? and \"A0524\" = '1'",a0000);
    }


    /**
     * 人员a06详细信息
     * @param a0000 人员唯一标识
     * @return 人员职务职级信息集合
     */
    public List<Record> findPgA06InfoByA0000(String a0000) {
        return Db.use(DBConstant.PG).find("select * from \"a06\" where \"A0000\" = ? and \"A0699\" = '1'",a0000);
    }

    /**
     * 人员a06详细信息
     * @param a0000 人员唯一标识
     * @return 人员职务职级信息集合
     */
    public List<Record> findPgA08InfoByA0000(String a0000) {
        return Db.use(DBConstant.PG).find("select * from \"a08\" where \"A0000\" = ? ",a0000);
    }

    /**
     * 人员a14详细信息
     * @param a0000 人员唯一标识
     * @return 人员奖惩信息集合
     */
    public List<Record> findPgA14InfoByA0000(String a0000) {
        return Db.use(DBConstant.PG).find("select * from \"a14\" where \"A0000\" = ? ",a0000);
    }

    /**
     * 人员a15信息信息
     * @param a0000 人员唯一标示
     * @return 人员的考核信息集合
     */
    public List<Record> findPgA15InfoByA0000(String a0000) {
        return Db.use(DBConstant.PG).find("select * from \"a15\" where \"A0000\" = ? ",a0000);
    }
    /**
     * 人员a36信息信息
     * @param a0000 人员唯一标示
     * @return 人员的考核信息集合
     */
    public List<Record> findPgA36InfoByA0000(String a0000) {
        return Db.use(DBConstant.PG).find("select * from \"a36\" where \"A0000\" = ? ",a0000);
    }

    /**
     * 查询sqlserverA44
     * @param leader_code 人员的唯一id
     * @return 人员的简历信息
     */
    public List<Record> findA44List(String leader_code) {
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[a44] where leader_code = ? order by startdatetime",leader_code);
    }

    /**
     * 查询sqlserverAcq03
     * @return
     */
    public List<Record> findSqlServerAcq03(String leader_code){
        return Db.use(DBConstant.SQLSERVER).find("select * from [区县02涪陵区].[dbo].[acq03] where acq0302 is not null and acq0302 <> '' and leader_code = ? order by acq0301 desc,acq0302 desc",leader_code);
    }

    /**
     * 查询sqlserverAcq03
     * @return
     */
    public Record findSqlServerAphotoByLeaderCode(String leaderCode){
        return Db.use(DBConstant.SQLSERVER).findFirst("select * from [区县02涪陵区].[dbo].[aphoto] where  leader_code = ? ",leaderCode);
    }
}