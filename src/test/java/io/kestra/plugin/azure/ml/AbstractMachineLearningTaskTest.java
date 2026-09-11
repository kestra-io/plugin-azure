package io.kestra.plugin.azure.ml;

import io.kestra.core.models.property.Property;

/**
 * Shared workspace coordinates for the credential-gated integration tests in this package, resolved from
 * `globals.azure.ml.*` at test-run time — matching the convention used by `datafactory` and `synapse` tests in
 * this repo. These tests are `@Disabled` by default since they require a live Azure Machine Learning workspace.
 */
abstract class AbstractMachineLearningTaskTest {
    protected static final Property<String> TENANT_ID = Property.ofExpression("{{ globals.azure.ml.tenantId }}");
    protected static final Property<String> CLIENT_ID = Property.ofExpression("{{ globals.azure.ml.clientId }}");
    protected static final Property<String> CLIENT_SECRET = Property.ofExpression("{{ globals.azure.ml.clientSecret }}");
    protected static final Property<String> SUBSCRIPTION_ID = Property.ofExpression("{{ globals.azure.ml.subscriptionId }}");
    protected static final Property<String> RESOURCE_GROUP_NAME = Property.ofExpression("{{ globals.azure.ml.resourceGroupName }}");
    protected static final Property<String> WORKSPACE_NAME = Property.ofExpression("{{ globals.azure.ml.workspaceName }}");
    protected static final Property<String> COMPUTE_NAME = Property.ofExpression("{{ globals.azure.ml.computeName }}");
    protected static final Property<String> ENVIRONMENT_ID = Property.ofExpression("{{ globals.azure.ml.environmentId }}");
}
