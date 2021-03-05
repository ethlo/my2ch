package my2ch;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.springframework.stereotype.Component;

import com.ethlo.my2ch.config.ClickHouseConfig;
import com.ethlo.my2ch.config.MysqlConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import picocli.CommandLine;

@Component
@CommandLine.Command(name = "transferCommand")
public class TransferCommand implements Callable<Integer>
{
    @CommandLine.Option(names = "--names", description = "The name of config(s) to run. Undefined runs all", required = false)
    private List<String> names;

    @CommandLine.Option(names = "--home", description = "The base directory to read config files from", required = true)
    private Path home;

    public Integer call() throws Exception
    {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory()).registerModule(new JavaTimeModule());

        final Map<String, String> envVariables = System.getenv();

        final Path mysqlConfigPath = home.resolve("mysql.yml");
        final MysqlConfig mysqlConfig= mapper.readValue(mysqlConfigPath.toFile(), MysqlConfig.class);

        final Path clickHouseConfigPath = home.resolve("clickhouse.yml");
        final ClickHouseConfig clickhouseConfig= mapper.readValue(mysqlConfigPath.toFile(), ClickHouseConfig.class);

        //final My2ch my2ch = new My2ch();

        return 0;
    }
}