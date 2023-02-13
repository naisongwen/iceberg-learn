package flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class TestFlinkRemote {

    @Test
    public void testSubmit() throws Exception {
        String host="dlink-project1261-flink-session1860-jobmanager";
        int port =80;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(host,port);
        DataStreamSource<Long> sequenceSource = env.fromSequence(1, 20);
        sequenceSource.print();
        // 执行job
        env.execute("source test job");
    }
}
