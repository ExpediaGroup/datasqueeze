package com.expedia.edw.data.squeeze;

import com.expedia.edw.data.squeeze.models.CompactionResponse;
import com.expedia.edw.data.squeeze.models.FileType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for {@link Utility}
 *
 * @author Yashraj R. Sontakke
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Utility.class, CompactionManager.class, CompactionManagerFactory.class})
public class UtilityTest {

    private final CompactionManager compactionManager = mock(CompactionManager.class);
    private final CompactionResponse response = new CompactionResponse(true, "target/path", FileType.ORC);
    private final Utility utility = new Utility();

    @Before
    public void setup() throws Exception {
        PowerMockito.mockStatic(CompactionManagerFactory.class);
        when(CompactionManagerFactory.create(Matchers.anyMap())).thenReturn(compactionManager);
        when(compactionManager.compact()).thenReturn(response);
    }

    @Test
    public void testUtility() throws Exception {
        String[] args = {"-sp", "source/path", "-tp", "target/path"};
        utility.main(args);
    }

    @Test
    public void testUtilityInPlace() throws Exception {
        String[] args = {"-sp", "source/path"};
        utility.main(args);
    }

    @Test
    public void testUtilityWithThreshold() throws Exception {
        String[] args = {"-sp", "source/path", "-tp", "target/path", "-threshold", "1234"};
        utility.main(args);
    }

    @Test
    public void testUtilityWithMaxReducers() throws Exception {
        String[] args = {"-sp", "source/path", "-tp", "target/path", "-maxReducers", "1234"};
        utility.main(args);
    }

    @Test
    public void testUtilityWithFileTypeAndSchemaPath() throws Exception {
        String[] args = { "-sp", "source/path", "-tp", "target/path", "-fileType", "AVRO", "-schemaPath", "schema/path" };
        utility.main(args);
    }
}
