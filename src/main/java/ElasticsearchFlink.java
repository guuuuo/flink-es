import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

public class ElasticsearchFlink {
    public static void main(String[] args) {
        // Create Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define a source
        try {
            DataStreamSource<String> source = env.socketTextStream("socket.amoylabs.com", 3000);

            DataStream<String> filterSource = source.filter((FilterFunction<String>) s -> !s.contains("hello"));

            DataStream<User> transSource = filterSource.map(value -> {
                String[] fields = value.split(",");
                return new User(fields[0], fields[1]);
            });

            transSource.sinkTo(
                    new Elasticsearch7SinkBuilder<User>()
                    .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                    .setHosts(new HttpHost("es.amoylabs.com", 30443, "https"))
                    .setEmitter((element, context, indexer) -> indexer.add(createIndexRequest(element)))
                    .build()
            );

            // Execute the transform
            env.execute("flink-es");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static IndexRequest createIndexRequest(User user) {
        Map<String, String> jsonMap = new HashMap<>();
        jsonMap.put("id", user.id);
        jsonMap.put("name", user.name);

        return Requests.indexRequest()
                .index("flink-es")
                .id(user.id)
                .source(jsonMap);
    }
}
