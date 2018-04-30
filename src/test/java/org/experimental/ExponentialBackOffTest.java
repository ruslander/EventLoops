package org.experimental;

import org.experimental.recoverability.ExponentialBackOff;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExponentialBackOffTest {
    @Test
    public void nextTimeout() throws Exception {
        Assert.assertEquals(ExponentialBackOff.nextTimeout(0), 100);
        Assert.assertEquals(ExponentialBackOff.nextTimeout(1), 200);
        Assert.assertEquals(ExponentialBackOff.nextTimeout(2), 400);
    }
}
