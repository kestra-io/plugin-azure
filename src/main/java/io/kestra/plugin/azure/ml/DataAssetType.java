package io.kestra.plugin.azure.ml;

/**
 * Kind of Azure Machine Learning data asset being registered or listed.
 */
public enum DataAssetType {
    URI_FILE,
    URI_FOLDER,
    MLTABLE
}
