package io.kestra.plugin.azure.ml;

/**
 * Kind of Azure Machine Learning data asset being registered or listed.
 */
public enum DataAssetType {
    URI_FILE("uri_file"),
    URI_FOLDER("uri_folder"),
    MLTABLE("mltable");

    private final String wireValue;

    DataAssetType(String wireValue) {
        this.wireValue = wireValue;
    }

    /**
     * The lowercase value Azure ML's SDK ({@code DataType.fromString(...)}) actually registers — the enum's Java
     * name (e.g. {@code URI_FILE}) is never a valid wire value on its own.
     */
    public String wireValue() {
        return wireValue;
    }
}
