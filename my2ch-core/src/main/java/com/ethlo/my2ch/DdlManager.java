package com.ethlo.my2ch;

import com.ethlo.my2ch.config.LifeCycle;

import java.nio.file.Path;

public interface DdlManager
{
    void run(final Path home, String alias, LifeCycle lifeCycle);
}
