package org.experimental;

import java.util.ArrayList;
import java.util.List;

public class UnitOfWork {
    private List<Runnable> callbacks = new ArrayList<>();

    public void register(Runnable cb) {
        callbacks.add(cb);
    }

    public void complete()
    {
        callbacks.forEach(cb -> cb.run());
        clear();
    }

    public void clear()
    {
        this.callbacks.clear();
    }
}
