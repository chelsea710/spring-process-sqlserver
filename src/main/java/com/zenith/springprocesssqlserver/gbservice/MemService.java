package com.zenith.springprocesssqlserver.gbservice;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;



/**
 * @author LHR
 * @date 2021/4/13
 */
@Service
public class MemService {

    @Value("${sync.gbservice.url}")
    private String gbServiceUrl;

    public boolean batchSyncMem(InMessage<BatchUpdateMem> inMessage) {

        HttpResponse execute = HttpRequest.post(gbServiceUrl + "/mem/batchSyncMem")
                .body(JSON.toJSONString(inMessage))
                .execute();
        String body = execute.body();
        System.out.println(body);
        return true;
    }
}
