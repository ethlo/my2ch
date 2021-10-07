package com.ethlo.my2ch;

import java.nio.file.Path;

import com.ethlo.my2ch.config.LifeCycle;

public interface DdlManager
{
    void run(final Path dir, LifeCycle lifeCycle);
}
