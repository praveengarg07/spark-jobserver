package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JHiveJob;
import java.util.List;

public class JHiveTestJob implements JHiveJob<Row[]> {

    @Override
    public Row[] run(HiveContext sc, JobEnvironment runtime, Config data) {
        List<Row> list = sc.sql(data.getString("sql")).collectAsList();
	return list.toArray(new Row[list.size()]);
    }

    @Override
    public Config verify(HiveContext sc, JobEnvironment runtime, Config config) {
        return config;
    }
}
