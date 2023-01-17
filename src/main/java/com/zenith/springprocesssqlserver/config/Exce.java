package com.zenith.springprocesssqlserver.config;

import com.zenith.springprocesssqlserver.pojo.CsvReader;
import com.zenith.springprocesssqlserver.sync.service.ProcessYZ;
import com.zenith.springprocesssqlserver.sync.service.SyncService;
import com.zenith.springprocesssqlserver.sync.service.SyncSingleService;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.crypto.*;
import java.io.*;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;


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
//        syncService.upload2019Pic();
//        syncService.uploadPic();
        syncService.processChangeData();
        syncService.result();
        System.out.println("运行完了");
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
//        cipher.init(Cipher.ENCRYPT_MODE,secretKey);
//        cipher.init(Cipher.DECRYPT_MODE,secretKey);

//        InputStream is = new FileInputStream("D:\\re2.zip");
//        OutputStream out = new FileOutputStream("D:\\re2.bkrs");
//        CipherInputStream cos = new CipherInputStream(is, cipher);
//        byte[] buffer = new byte[1024];
//        int r;
//        while ((r = cos.read(buffer)) >= 0)
//            out.write(buffer, 0, r);
//        cos.close();
//        out.close();
//        InputStream is = new FileInputStream(new File("C:\\Users\\48951\\Desktop\\按机构导出文件_E09.443_重庆市规划和自然资源局_20230107174132.bkrs"));
//        OutputStream out = new FileOutputStream(new File("C:\\Users\\48951\\Desktop\\re8.zip"));
//        // 解密
//        CipherOutputStream cos = new CipherOutputStream(out, cipher);
//        byte[] buffer = new byte[1024];
//        int r;
//        while ((r = is.read(buffer)) >= 0)
//            cos.write(buffer, 0, r);
//        cos.close();
//        out.close();
//        is.close();

//        is.close();
//        syncService.calc();
//        syncService.calc2();
//        syncService.processExcelDict();
//        syncService.processExcelOladDict();
//        syncService.changeField();
//        syncService.processCodeValueBySalay();

//        this.dencrpFile("C:\\Users\\48951\\Downloads\\按机构导出文件_E09.G49.X06_重庆市合川区小沔镇人民政府_20221211061246.bkrs","D:\\re.zip");
//        System.exit(0);


//        String std = "10989#@$10230#@$9954#@$6366#@$6435#@$6228#@$6573#@$6228#@$6642#@$";
//        String st2 = "8988#@$6642#@$6228#@$11196#@$6297#@$7746#@$6366#@$9540#@$6435#@$11196#@$6504#@$9678#@$6228#@$6366#@$9540#@$6573#@$";
//        String ps = "8988#@$6711#@$6297#@$11196#@$6366#@$7746#@$6435#@$9540#@$6504#@$11196#@$6573#@$9678#@$6297#@$6435#@$9540#@$6642#@$";
//        System.out.println( this.decryptHZB(st2));
//        System.out.println( this.decrypt(ps));

//        CsvReader cw = new CsvReader("E:\\RS_A01.txt", '|', Charset.forName("UTF-8"));
//        cw.readHeaders();
//        while (cw.readRecord()) {
//            String[] values = cw.getValues();
//            System.out.println(values.length);
//            if(values.length != 138){
//                System.out.println(values);
//            }
//        }
    }


    public String decryptHZB(String ssoToken) {
        try {
            String decrypt = "#@$";
            String name = "";
            StringTokenizer st = new StringTokenizer(ssoToken, decrypt);
            while (st.hasMoreElements()) {
                int asc = (Integer.parseInt((String)st.nextElement()) - 87) / 69 - 39;
                name = name + (char)asc;
            }
            return name;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String decrypt(String ssoToken) {
        try {
            String decrypt = "#@$";
            String name = "";
            StringTokenizer st = new StringTokenizer(ssoToken, decrypt);
            while (st.hasMoreElements()) {
                int asc = (Integer.parseInt((String)st.nextElement()) - 87) / 69 - 39;
                name = name + (char)asc;
            }
            return name;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 解密
     *
     * @param inputFilePath
     * @param outputFilePath
     * @throws Exception
     */
    public  void dencrpFile(String inputFilePath, String outputFilePath) throws Exception {
        String Encrypt = "2d0a0ba4c857b8faad98663edcab76757b3fdeb00544fd737e98a55c147422d050643f179d67ef800bfab6a0408db7df0bfab6a0408db7df21bc8b0db3aa1828";
        MessageDigest alg = MessageDigest.getInstance("MD5");
        alg.update(Encrypt.getBytes("iso-8859-1"));
        byte[] result = alg.digest();

        String strKey = byte2hex(result);

        KeyGenerator keyGenerator = KeyGenerator.getInstance("DES");
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(strKey.getBytes("iso-8859-1"));
        keyGenerator.init(56, secureRandom);
        SecretKey secretKey = keyGenerator.generateKey();
        Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
        cipher.init(2, secretKey);

        InputStream is = new FileInputStream(inputFilePath);
        OutputStream out = new FileOutputStream(outputFilePath);
        // 解密
        CipherOutputStream cos = new CipherOutputStream(out, cipher);
        byte[] buffer = new byte[1024];
        int r;
        while ((r = is.read(buffer)) >= 0)
            cos.write(buffer, 0, r);
        cos.close();
        out.close();
        is.close();
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