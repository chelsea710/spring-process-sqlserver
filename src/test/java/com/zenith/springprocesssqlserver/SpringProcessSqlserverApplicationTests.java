package com.zenith.springprocesssqlserver;

import com.zenith.springprocesssqlserver.gbservice.A0000Dto;
import com.zenith.springprocesssqlserver.gbservice.BatchUpdateMem;
import com.zenith.springprocesssqlserver.gbservice.InMessage;
import com.zenith.springprocesssqlserver.gbservice.MemService;
import com.zenith.springprocesssqlserver.sync.service.StrDateFormatUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class SpringProcessSqlserverApplicationTests {

    @Autowired
    MemService memService;

    @Test
    void contextLoads() {
        InMessage<BatchUpdateMem> batchUpdateMemInMessage = new InMessage<>();
        BatchUpdateMem batchUpdateMem = new BatchUpdateMem();
        batchUpdateMem.setIsA08("true");
        List<A0000Dto> param = new ArrayList<>();
        A0000Dto a0000Dto = new A0000Dto();
        a0000Dto.setA0000("585bc571bca04caba56b309b38251c54");
        param.add(a0000Dto);
        batchUpdateMem.setMems(param);
        batchUpdateMemInMessage.setData(batchUpdateMem);
        memService.batchSyncMem(batchUpdateMemInMessage);
    }

    @Test
    public void test(){
        String dateFormat = StrDateFormatUtil.getDateFormat("2019/11/21");
        System.out.println(dateFormat);
    }

}
