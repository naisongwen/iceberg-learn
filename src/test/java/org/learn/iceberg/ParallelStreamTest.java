package org.learn.iceberg;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertThat;

public class ParallelStreamTest {
    //https://blog.csdn.net/sl1992/article/details/100149187
    @Test
    public void whenConvertedToList_thenCorrect() {

        Iterable<String> iterable
                = Arrays.asList("Testing", "Iterable", "conversion", "to", "Stream");

        List<String> result = StreamSupport.stream(iterable.spliterator(), false)
                .map(String::toUpperCase)
                .collect(Collectors.toList());

        //assertThat(result, contains("TESTING", "ITERABLE", "CONVERSION", "TO", "STREAM"));
    }
}
