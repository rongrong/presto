package com.facebook.presto.spi.function;

import com.facebook.presto.spi.api.Experimental;

@Experimental
public interface FunctionImplementation
{
    FunctionLanguage getLanguage();

    Object getImplementation();
}
