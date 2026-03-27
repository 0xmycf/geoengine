use geoengine_datatypes::raster::{GridBoundingBox2D, RasterDataType};
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum GdalSourceError {
    #[snafu(display("Unsupported raster type: {raster_type:?}"))]
    UnsupportedRasterType { raster_type: RasterDataType },

    #[snafu(display("Unsupported spatial query: {spatial_query:?}"))]
    IncompatibleSpatialQuery { spatial_query: GridBoundingBox2D },


    #[snafu(display("GdalProcess returned error {} instead of tiles", error_kind))]
    ProcessInternalBincodeError {
        error_kind: String,
    },

    #[snafu(display("GdalProcess returned Io error {internal_error}"))]
    ProcessInternalIoError {
        internal_error: String,
    },

    #[snafu(display("GdalProcess is disconnected or not running."))]
    ProcessIsDisconnected,

    #[snafu(display("GdalProcess failed while converting tiles from or to the binary ipc arrow format: {reason}"))]
    IpcArrowConversionFailed {
        reason: String
    },

    #[snafu(display("GdalProcess with an unknown error: {error}"))]
    UnknownErrorHappenedWhileReading {
        error: String
    }

}
