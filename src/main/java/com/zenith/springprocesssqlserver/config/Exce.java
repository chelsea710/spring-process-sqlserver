package com.zenith.springprocesssqlserver.config;

import com.zenith.springprocesssqlserver.sync.service.ProcessYZ;
import com.zenith.springprocesssqlserver.sync.service.SyncService;
import com.zenith.springprocesssqlserver.sync.service.SyncSingleService;
import org.apache.commons.codec.digest.Crypt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.SecureRandom;


/**
 * @author LHR
 * @date 20210326
 */
@Component
@Order(1)
public class Exce implements ApplicationRunner {

    @Autowired
    private SyncService syncService;

    @Autowired
    private SyncSingleService syncSingleService;

    @Autowired
    private ProcessYZ processYZ;

    @Override
    public void run(ApplicationArguments args) throws Exception {
//        syncService.syncA01();
//        syncService.syncA02();
//        syncService.syncA05();
//        syncService.syncA06();
//        syncService.syncA08();
//        syncService.syncA14();
//        syncService.syncA15();
//        syncService.syncA36();
//        syncService.syncB01();
//        syncService.syncphotos();
//        syncService.processQxRole();
        //同步一下身份证和A0000
//        syncService.syncLeaderCode();
        //开始同步
//        while (true){
//            Thread.sleep(500);
//            syncSingleService.sync();
//        }
//        processYZ.a360818YzSort();

//        List<Record> records = Db.use(DBConstant.SQLSERVER).find("select *  from [gb].[a01]");
//        System.out.println(records);
        //替换简历不连续
//        syncService.processA1701();
        //拉取照片
//        syncService.uploadPic();
//        syncService.processChangeData();
//        syncService.result();
//        String Encrypt = "2d0a0ba4c857b8faad98663edcab76757b3fdeb00544fd737e98a55c147422d050643f179d67ef800bfab6a0408db7df0bfab6a0408db7df21bc8b0db3aa1828";
//        MessageDigest alg = MessageDigest.getInstance("MD5");
//        alg.update(Encrypt.getBytes("iso-8859-1"));
//        byte[] result = alg.digest();
//
//        String strKey = byte2hex(result);
//
//        KeyGenerator keyGenerator = KeyGenerator.getInstance("DES");
//        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
//        secureRandom.setSeed(strKey.getBytes("iso-8859-1"));
//        keyGenerator.init(56,secureRandom);
//        SecretKey secretKey = keyGenerator.generateKey();
//        Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
//        cipher.init(2,secretKey);
//
//        InputStream is = new FileInputStream("C:\\Users\\48951\\Desktop\\基础人员数据_重庆市沙坪坝区丰文街道(0000044000040)_2022-01-27.bkrs");
//        OutputStream out = new FileOutputStream("D:\\re");
//        CipherOutputStream cos = new CipherOutputStream(out, cipher);
//        byte[] buffer = new byte[1024];
//        int r;
//        while ((r = is.read(buffer)) >= 0)
//            cos.write(buffer, 0, r);
//        cos.close();
//        out.close();
//        is.close();

//        syncService.calc();
//        syncService.calc2();
        syncService.processExcelDict();
        syncService.processExcelOladDict();
//        syncService.changeField();
//        syncService.processCodeValueBySalay();
        System.exit(0);

    }

    public  String byte2hex(byte[] b) {
        String hs = "";
        String stmp = "";
        for (int n = 0; n < b.length; n++) {
            stmp = Integer.toHexString(b[n] & 0xFF);
            if (stmp.length() == 1) {
                hs = hs + "0" + stmp;
            } else {
                hs = hs + stmp;
            }
            if (n < b.length - 1)
                hs = hs + "";
        }
        return hs.toUpperCase();
    }

}