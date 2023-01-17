package com.zenith.springprocesssqlserver.thread;

import com.jfinal.plugin.activerecord.Record;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Callable;

/**
 *
 * 多线程查询数据用
 * @date 20220808
 * @author LHR
 */
@Component
public class TaskThreadQuery implements Callable<List<Record>> {

    @Override
    public List<Record> call() throws Exception {
            return null;
    }
}
