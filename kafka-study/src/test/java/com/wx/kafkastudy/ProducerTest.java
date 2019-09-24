package com.wx.kafkastudy;

import com.wx.kafkastudy.producter.Producter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ProducerTest {

    @Autowired
    private Producter producter;

    @Test
    public void producer(){
        for(int i=0;i<100;i++){
            producter.productDate("zhangsan");
        }

    }

}
