package com.zenith.springprocesssqlserver.sync.service;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;
import com.zenith.springprocesssqlserver.constant.DBConstant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author LHR
 * @date 2021/4/9
 */
@Service
public class DictionaryService {

//    @Autowired
//    public Map<String,List<Record>> dicMap;

    public Map<String,List<Record>> dicMap;


    public List<String> getDictionaryValue(String codeType) {
        List<String> result = new ArrayList<>();
        List<Record> records = this.getDicMap().get(codeType);
        for (Record record : records) {
            String codeValue = record.getStr("CODE_VALUE");
            result.add(codeValue);
        }
        return result;
    }

    public Map<String,List<Record>> getDicMap(){
        if(ObjectUtil.isNotNull(dicMap) && dicMap.size() > 0){
            return dicMap;
        } else {
            Map<String, List<Record>> collect = Db.use(DBConstant.PG).find("select * from \"code_value\"").stream()
                    .filter(var -> StrUtil.isNotEmpty(var.getStr("CODE_TYPE")))
                    .collect(Collectors.groupingBy(var -> var.getStr("CODE_TYPE")));
            dicMap = collect;
            return dicMap;
        }
    }


    public Set<String> getSet(String zb64) {
        return this.getDicMap().get(zb64).stream().map(var->var.getStr("CODE_VALUE")).collect(Collectors.toSet());
    }

    public Map<String,String> getDicMap(String codeType) {
        return this.getDicMap().get(codeType).stream().collect(Collectors.toMap(key->key.getStr("CODE_VALUE"),value->value.getStr("CODE_NAME"),(key1,key2)->key1));
    }
}
