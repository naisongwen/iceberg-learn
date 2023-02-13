package org.learn.iceberg;

import org.junit.Test;

public class TestUtil {
    @Test
    public void test(){
        StringBuffer sb=new StringBuffer("/tmp/iceberg/default/");
        do{
            sb.deleteCharAt(0);
        }
        while (sb.charAt(0) !='/');
        sb.deleteCharAt(0);
        System.out.println(sb);
    }
}
