package io.kestra.plugin.azure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// Marks tests on features Floci does not emulate: they run against the Azure account from the CI config, and skip without one.
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface RequiresRealAzure {
    // Why the emulator cannot run the test.
    String value();
}
