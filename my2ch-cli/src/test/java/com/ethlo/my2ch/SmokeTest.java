package com.ethlo.my2ch;

/*-
 * #%L
 * my2ch
 * %%
 * Copyright (C) 2021 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.ethlo.my2ch.config.TransferConfig;

@SpringBootTest(classes = Cfg.class)
@RunWith(SpringRunner.class)
public class SmokeTest
{
    private static final Logger logger = LoggerFactory.getLogger(SmokeTest.class);

    @Test
    public void testTransfer() throws IOException
    {
        final Path baseDir = Paths.get("/home/morten/dev/ethlo/my2ch/java/my2ch-cli/src/test/resources/samples");
        final String alias = "foo1";
        final TransferConfig config = My2chConfigLoader.loadConfig(baseDir, alias);
        final My2ch my2ch = new My2ch(config);
        final long transferred = my2ch.run(queryProgress ->
        {
            logger.info("Progress: {}", queryProgress);
            return true;
        });
        logger.info("Transferred: {}", transferred);
        logger.info("Table stats: {}", my2ch.getStats());
    }
}
