package org.experimental;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class UnitOfWorkTest {

    @Test
    public void complete_will_fire_callbacks_test() throws Exception {

        AtomicInteger cnt = new AtomicInteger(0);

        UnitOfWork uow =  new UnitOfWork();
        uow.register(()-> {cnt.incrementAndGet();});

        uow.complete();

        Assert.assertEquals(cnt.get(), 1);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void error_will_propagate_test() {
        UnitOfWork uow =  new UnitOfWork();
        uow.register(()-> {throw  new RuntimeException("booom");});

        uow.complete();
    }
}
