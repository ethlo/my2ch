package com.ethlo.my2ch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.ethlo.clackshack.ClackShackImpl;
import com.ethlo.clackshack.util.IOUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@SpringBootTest(classes = Cfg.class)
@RunWith(SpringRunner.class)
public class SmokeTest
{
    @Test
    public void testLoad() throws JsonProcessingException
    {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.registerModule(new JavaTimeModule());
        final TransferConfig config = mapper.readValue(IOUtil.readClasspath("sample_transfer1.yaml"), TransferConfig.class);
        assertThat(config).isNotNull();
        System.out.println(new ObjectMapper().registerModule(new JavaTimeModule()).writerWithDefaultPrettyPrinter().writeValueAsString(config));

        final My2ch my2ch = new My2ch(new MysqlConfig("localhost", 3306, "root", "qwerty123", "ssp"), new ClackShackImpl("http://localhost:8123"));
        my2ch.processSingle(config);
        System.out.println(my2ch.getStats(config));
    }
}
