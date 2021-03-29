package feast.storage.connector.cassandra.retriever;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

public class OnlineRetriever implements OnlineRetrieverV2 {

  private final CqlSession session;
  private final String keyspace;

  public OnlineRetriever(String connectionString, String datacenter, String keyspace) {
    List<InetSocketAddress> contactPoints = Arrays.stream(connectionString.split(","))
        .map(String::trim)
        .map(cs -> cs.split(":"))
        .map(hostPort -> new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])))
        .collect(Collectors.toList());
    
        this.session = new CqlSessionBuilder().addContactPoints(contactPoints).withLocalDatacenter(datacenter).build();
        this.keyspace = keyspace;
  }

  private Term


  @Override
  public List<List<Feature>> getOnlineFeatures(String project, List<ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow> entityRows, List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    List<String> featureColumns = featureReferences.stream().map(fr -> String.format("%s_%s", fr.getFeatureTable(), fr.getName())).collect(Collectors.toList());
    entityRows.stream()
        .map(ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow::getFieldsMap)
        .map(fm -> {
          String tableName = fm.keySet().stream().sorted().collect(Collectors.joining("_")).replace("-", "_");
          Select statement = selectFrom(keyspace, tableName).columns(featureColumns);
          fm.forEach((entityName, entityValue) -> {
            entityValue.getValCase();

            statement = statement.whereColumn(entityName).isEqualTo()
          });

        });
    selectFrom(keyspace, "tablename").column("*")
        .whereColumn("entity")
    return null;
  }
}
