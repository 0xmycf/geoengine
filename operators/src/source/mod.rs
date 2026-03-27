mod csv;
mod gdal_source;
mod multi_band_gdal_source;
mod ogr_source;

pub use self::csv::{
    CsvGeometrySpecification, CsvSource, CsvSourceParameters, CsvSourceStream, CsvTimeSpecification,
};

// __private has a deprecation warning, but must be exported for the bin
#[allow(deprecated)]
#[cfg(feature = "gdalsource-process")]
pub use self::gdal_source::__private;

pub use self::gdal_source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfo,
    GdalLoadingInfoTemporalSlice, GdalLoadingInfoTemporalSliceIterator, GdalMetaDataList,
    GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataMapping, GdalMetadataNetCdfCf,
    GdalRetryOptions, GdalSource, GdalSourceError, GdalSourceParameters, GdalSourceProcessor,
    GdalSourceTimePlaceholder, TimeReference,
    process::{
        GdalDatasetCache, IpcChannelMessage, IpcChannelMessagePayload, IpcProcessError,
        IpcProcessRasterResult, ProcessData, ProcessManager, TileData, setup_client,
        setup_client_for_bytes,
    },
};
pub use self::multi_band_gdal_source::{
    GdalMultiBand, GdalSourceError as MultiBandGdalSourceError,
    GdalSourceParameters as MultiBandGdalSourceParameters, MultiBandGdalLoadingInfo,
    MultiBandGdalLoadingInfoQueryRectangle, MultiBandGdalSource, TileFile,
};
pub use self::ogr_source::{
    AttributeFilter, CsvHeader, FormatSpecifics, OgrSource, OgrSourceColumnSpec, OgrSourceDataset,
    OgrSourceDatasetTimeType, OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceParameters,
    OgrSourceProcessor, OgrSourceTimeFormat, UnixTimeStampType,
};
